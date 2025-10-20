package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/database"
)

type Secondary struct {
	db           *database.Database
	primaryURL   string
	client       *http.Client
	stop         chan struct{}
	done         chan struct{}
	progress     map[string]int64
	progressMu   sync.RWMutex
	progressFile string
}

func NewSecondary(db *database.Database, primaryURL string) *Secondary {
	primary := strings.TrimRight(primaryURL, "/")
	if !strings.HasPrefix(primary, "http://") && !strings.HasPrefix(primary, "https://") {
		primary = "http://" + primary
	}
	s := &Secondary{
		db:         db,
		primaryURL: primary,
		client:     &http.Client{},
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		progress:   map[string]int64{},
	}
	if db != nil && db.Config != nil {
		s.progressFile = filepath.Join(db.Config.Dir, ".replication-progress.json")
	}
	s.loadProgress()
	return s
}

func (s *Secondary) Start() {
	go s.loop()
}

func (s *Secondary) Stop() {
	close(s.stop)
	<-s.done
}

func (s *Secondary) loop() {
	defer close(s.done)

	for {
		if s.db.GetStatus() != database.StatusOperating {
			select {
			case <-s.stop:
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		err := s.runOnce()
		if err != nil && !isContextError(err) {
			fmt.Println("replication error:", err)
		}

		select {
		case <-s.stop:
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *Secondary) runOnce() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.primaryURL+"/v1/replication/stream", nil)
	if err != nil {
		return err
	}

	query := req.URL.Query()
	for name, position := range s.snapshotPositions() {
		if position <= 0 {
			continue
		}
		query.Add("since", fmt.Sprintf("%s:%d", name, position))
	}
	req.URL.RawQuery = query.Encode()

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("replication stream failed: %s", strings.TrimSpace(string(body)))
	}

	decoder := json.NewDecoder(resp.Body)

	for {
		var event Event
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				return nil
			}
			if isContextError(err) {
				return err
			}
			return fmt.Errorf("decode event: %w", err)
		}

		if err := s.applyEvent(&event); err != nil {
			return err
		}
	}
}

func (s *Secondary) applyEvent(event *Event) error {
	if event.Collection == "" {
		return fmt.Errorf("replication event missing collection name")
	}

	var command collection.Command
	if err := json.Unmarshal(event.Command, &command); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	col, ok := s.db.Collections[event.Collection]
	if !ok {
		var err error
		col, err = s.db.CreateCollection(event.Collection)
		if err != nil {
			return fmt.Errorf("create collection '%s': %w", event.Collection, err)
		}
	}

	remoteStart := command.StartByte

	if s.lastApplied(event.Collection) > remoteStart {
		return nil
	}

	if err := col.ApplyCommand(&command, true); err != nil {
		if s.handleOutOfSync(event.Collection, err) {
			return fmt.Errorf("collection '%s' reset for resync: %w", event.Collection, err)
		}
		return fmt.Errorf("apply command on collection '%s': %w", event.Collection, err)
	}

	s.recordProgress(event.Collection, remoteStart+1)

	return nil
}

func (s *Secondary) snapshotPositions() map[string]int64 {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()

	positions := make(map[string]int64, len(s.progress))
	for name, position := range s.progress {
		if position > 0 {
			positions[name] = position
		}
	}
	return positions
}

func isContextError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isContextError(urlErr.Err)
	}
	return false
}

func (s *Secondary) loadProgress() {
	if s.progressFile == "" {
		return
	}

	data, err := os.ReadFile(s.progressFile)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("replication warning: load progress:", err)
		}
		return
	}

	entries := map[string]int64{}
	if err := json.Unmarshal(data, &entries); err != nil {
		fmt.Println("replication warning: decode progress:", err)
		return
	}

	s.progressMu.Lock()
	for name, position := range entries {
		if position > 0 {
			s.progress[name] = position
		}
	}
	s.progressMu.Unlock()
}

func (s *Secondary) saveProgressLocked() {
	if s.progressFile == "" {
		return
	}

	data, err := json.Marshal(s.progress)
	if err != nil {
		fmt.Println("replication warning: encode progress:", err)
		return
	}

	tmp := s.progressFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		fmt.Println("replication warning: persist progress:", err)
		return
	}
	if err := os.Rename(tmp, s.progressFile); err != nil {
		fmt.Println("replication warning: finalize progress:", err)
	}
}

func (s *Secondary) lastApplied(name string) int64 {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()
	return s.progress[name]
}

func (s *Secondary) recordProgress(name string, position int64) {
	if position <= 0 {
		return
	}

	s.progressMu.Lock()
	if s.progress == nil {
		s.progress = map[string]int64{}
	}
	current := s.progress[name]
	if position > current {
		s.progress[name] = position
		s.saveProgressLocked()
	}
	s.progressMu.Unlock()
}

func (s *Secondary) handleOutOfSync(name string, err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	if !strings.Contains(message, "out of range") && !strings.Contains(message, "already exists") && !strings.Contains(message, "does not exist") {
		return false
	}

	col, ok := s.db.Collections[name]
	if ok {
		_ = col.Close()
		delete(s.db.Collections, name)
	}

	if s.db.Config != nil {
		path := filepath.Join(s.db.Config.Dir, name)
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			fmt.Println("replication warning: reset collection:", removeErr)
		}
	}

	s.progressMu.Lock()
	delete(s.progress, name)
	s.saveProgressLocked()
	s.progressMu.Unlock()

	return true
}
