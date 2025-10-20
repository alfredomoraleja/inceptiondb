package replication

import (
	"context"
	"io"
	"net/http"
	"strings"
)

type Forwarder struct {
	primaryURL string
	client     *http.Client
}

func NewForwarder(primaryURL string) *Forwarder {
	url := strings.TrimRight(primaryURL, "/")
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	return &Forwarder{
		primaryURL: url,
		client:     &http.Client{},
	}
}

func (f *Forwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) (bool, error) {
	if f == nil {
		return false, nil
	}

	req, err := http.NewRequestWithContext(ctx, r.Method, f.primaryURL+r.URL.RequestURI(), r.Body)
	if err != nil {
		return true, err
	}

	req.Header = r.Header.Clone()

	resp, err := f.client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)

	_, err = io.Copy(w, resp.Body)
	return true, err
}
