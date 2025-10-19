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

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	events, cancel := manager.Subscribe()
	defer cancel()

	encoder := json.NewEncoder(w)

	if err := sendHistory(r.Context(), db, encoder, flusher); err != nil {
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

func sendHistory(ctx context.Context, db *database.Database, encoder *json.Encoder, flusher http.Flusher) error {
	for name, col := range db.Collections {
		if err := streamCollectionHistory(ctx, name, col, encoder, flusher); err != nil {
			return err
		}
	}
	return nil
}

func streamCollectionHistory(ctx context.Context, name string, col *collection.Collection, encoder *json.Encoder, flusher http.Flusher) error {
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

		command := &collection.Command{}
		if err := json2.UnmarshalDecode(decoder, command); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode command: %w", err)
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
