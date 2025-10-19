package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/database"
)

type Secondary struct {
	db         *database.Database
	primaryURL string
	client     *http.Client
	stop       chan struct{}
	done       chan struct{}
}

func NewSecondary(db *database.Database, primaryURL string) *Secondary {
	primary := strings.TrimRight(primaryURL, "/")
	if !strings.HasPrefix(primary, "http://") && !strings.HasPrefix(primary, "https://") {
		primary = "http://" + primary
	}
	return &Secondary{
		db:         db,
		primaryURL: primary,
		client:     &http.Client{},
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
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

	if err := col.ApplyCommand(&command, true); err != nil {
		return fmt.Errorf("apply command on collection '%s': %w", event.Collection, err)
	}

	return nil
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
