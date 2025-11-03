package driver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxErrorBody = 1 << 20 // 1MiB should be more than enough for error messages.

// Error represents an HTTP level error returned by the server.
type Error struct {
	StatusCode  int
	Message     string
	Description string
	Body        []byte
}

func (e *Error) Error() string {
	status := http.StatusText(e.StatusCode)
	if status == "" {
		status = fmt.Sprintf("status %d", e.StatusCode)
	}
	if e.Message == "" {
		if len(strings.TrimSpace(string(e.Body))) > 0 {
			return fmt.Sprintf("inceptiondb: %s: %s", status, strings.TrimSpace(string(e.Body)))
		}
		return fmt.Sprintf("inceptiondb: %s", status)
	}
	if e.Description != "" {
		return fmt.Sprintf("inceptiondb: %s: %s (%s)", status, e.Message, e.Description)
	}
	return fmt.Sprintf("inceptiondb: %s: %s", status, e.Message)
}

func parseErrorResponse(resp *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if err != nil {
		return &Error{StatusCode: resp.StatusCode, Body: data, Message: err.Error()}
	}

	var payload struct {
		Error struct {
			Message     string `json:"message"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &payload); err == nil {
			if payload.Error.Message != "" || payload.Error.Description != "" {
				return &Error{
					StatusCode:  resp.StatusCode,
					Message:     payload.Error.Message,
					Description: payload.Error.Description,
					Body:        data,
				}
			}
		}
	}

	message := strings.TrimSpace(string(data))
	return &Error{StatusCode: resp.StatusCode, Message: message, Body: data}
}
