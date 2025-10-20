package apireplicationv1

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	json2 "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/database"
	"github.com/fulldump/inceptiondb/replication"
)

func stream(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	manager := GetManager(ctx)
	if manager == nil {
		http.Error(w, "replication disabled", http.StatusServiceUnavailable)
		return nil
	}

	db := GetDatabase(ctx)
	if db == nil {
		return fmt.Errorf("database not available")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("streaming not supported")
	}

	offsets, err := parseSince(r.URL.Query()["since"])
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid since parameter: %v", err), http.StatusBadRequest)
		return nil
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	events, cancel := manager.Subscribe()
	defer cancel()

	encoder := json.NewEncoder(w)

	if err := sendHistory(r.Context(), db, encoder, flusher, offsets); err != nil {
		return err
	}

	for {
		select {
		case <-r.Context().Done():
			return nil
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if err := encoder.Encode(event); err != nil {
				return err
			}
			flusher.Flush()
		}
	}
}

func sendHistory(ctx context.Context, db *database.Database, encoder *json.Encoder, flusher http.Flusher, offsets map[string]int64) error {
	for name, col := range db.Collections {
		if err := streamCollectionHistory(ctx, name, col, encoder, flusher, offsets[name]); err != nil {
			return err
		}
	}
	return nil
}

func streamCollectionHistory(ctx context.Context, name string, col *collection.Collection, encoder *json.Encoder, flusher http.Flusher, skipUntil int64) error {
	file, err := os.Open(col.Filename)
	if err != nil {
		return fmt.Errorf("open collection '%s': %w", name, err)
	}
	defer file.Close()

	decoder := jsontext.NewDecoder(file,
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		startOffset := decoder.InputOffset()

		command := &collection.Command{}
		if err := json2.UnmarshalDecode(decoder, command); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode command: %w", err)
		}

		if command.StartByte == 0 {
			command.StartByte = startOffset
		}

		if skipUntil > 0 && command.StartByte < skipUntil {
			continue
		}

		payload, err := json.Marshal(command)
		if err != nil {
			return fmt.Errorf("encode command: %w", err)
		}

		event := &replication.Event{
			Collection: name,
			Command:    payload,
		}

		if err := encoder.Encode(event); err != nil {
			return err
		}
		flusher.Flush()
	}
}

func parseSince(values []string) (map[string]int64, error) {
	if len(values) == 0 {
		return nil, nil
	}

	offsets := make(map[string]int64, len(values))
	for _, value := range values {
		parts := strings.SplitN(value, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid token %q", value)
		}

		offset, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid offset for %s", parts[0])
		}

		if current, exists := offsets[parts[0]]; !exists || offset > current {
			offsets[parts[0]] = offset
		}
	}

	return offsets, nil
}
