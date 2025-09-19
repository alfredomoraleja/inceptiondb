package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

// Client is a high level HTTP client for the InceptionDB REST API.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// Option configures a Client instance.
type Option func(*Client)

// WithHTTPClient overrides the default HTTP client used to perform requests.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		c.httpClient = h
	}
}

// NewClient creates a new Client pointing to the provided base URL.
func NewClient(baseURL string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("base URL is required")
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base url: %w", err)
	}
	if parsed.Scheme == "" {
		return nil, errors.New("base URL must include the scheme (http or https)")
	}
	if parsed.Host == "" {
		return nil, errors.New("base URL must include the host")
	}

	c := &Client{
		baseURL:    parsed,
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
	return c, nil
}

// ListCollections retrieves the collections metadata available in the server.
func (c *Client) ListCollections(ctx context.Context) ([]Collection, error) {
	var collections []Collection
	if err := c.doJSON(ctx, http.MethodGet, "/v1/collections", nil, &collections); err != nil {
		return nil, err
	}
	return collections, nil
}

// CreateCollection creates a new collection and returns its metadata.
func (c *Client) CreateCollection(ctx context.Context, req *CreateCollectionRequest) (*Collection, error) {
	if req == nil {
		return nil, errors.New("create collection request is nil")
	}
	body, err := encodeJSONPayload(req)
	if err != nil {
		return nil, fmt.Errorf("encode create collection request: %w", err)
	}
	var result Collection
	if err := c.doJSON(ctx, http.MethodPost, "/v1/collections", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCollection retrieves the metadata of a single collection.
func (c *Client) GetCollection(ctx context.Context, collection string) (*Collection, error) {
	var result Collection
	if err := c.doJSON(ctx, http.MethodGet, collectionPath(collection), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DropCollection deletes the collection and its indexes.
func (c *Client) DropCollection(ctx context.Context, collection string) error {
	return c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "dropCollection"), nil, nil)
}

// SetDefaults configures the default document used when inserting new rows.
func (c *Client) SetDefaults(ctx context.Context, collection string, defaults map[string]any) (map[string]any, error) {
	body, err := encodeJSONObject(defaults)
	if err != nil {
		return nil, fmt.Errorf("encode defaults request: %w", err)
	}
	result := map[string]any{}
	if err := c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "setDefaults"), body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// ListIndexes returns the indexes registered in a collection.
func (c *Client) ListIndexes(ctx context.Context, collection string) ([]Index, error) {
	var result []Index
	if err := c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "listIndexes"), nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// CreateIndex registers a new index and returns its metadata.
func (c *Client) CreateIndex(ctx context.Context, collection string, req *CreateIndexRequest) (*Index, error) {
	if req == nil {
		return nil, errors.New("create index request is nil")
	}
	body, err := encodeJSONPayload(req)
	if err != nil {
		return nil, fmt.Errorf("encode create index request: %w", err)
	}
	var result Index
	if err := c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "createIndex"), body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetIndex retrieves information about a single index.
func (c *Client) GetIndex(ctx context.Context, collection, name string) (*Index, error) {
	body, err := encodeJSONObject(map[string]string{"name": name})
	if err != nil {
		return nil, fmt.Errorf("encode get index request: %w", err)
	}
	var result Index
	if err := c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "getIndex"), body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DropIndex removes an index from a collection.
func (c *Client) DropIndex(ctx context.Context, collection, name string) error {
	body, err := encodeJSONObject(map[string]string{"name": name})
	if err != nil {
		return fmt.Errorf("encode drop index request: %w", err)
	}
	return c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "dropIndex"), body, nil)
}

// Size returns statistics about the collection usage. This endpoint is experimental.
func (c *Client) Size(ctx context.Context, collection string) (map[string]any, error) {
	result := map[string]any{}
	if err := c.doJSON(ctx, http.MethodPost, collectionActionPath(collection, "size"), nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// InsertStream sends the provided JSON Lines payload to the insert endpoint and
// returns a stream with the inserted documents.
func (c *Client) InsertStream(ctx context.Context, collection string, reader io.Reader) (*JSONStream, error) {
	if reader == nil {
		reader = http.NoBody
	}
	return c.stream(ctx, http.MethodPost, collectionActionPath(collection, "insert"), reader, "application/json")
}

// InsertDocuments is a convenience helper that encodes the provided documents as
// JSON Lines before sending them to the server.
func (c *Client) InsertDocuments(ctx context.Context, collection string, documents ...any) (*JSONStream, error) {
	var reader io.Reader
	if len(documents) > 0 {
		r, err := encodeJSONLines(documents)
		if err != nil {
			return nil, fmt.Errorf("encode insert payload: %w", err)
		}
		reader = r
	}
	return c.InsertStream(ctx, collection, reader)
}

// Find executes a query against the collection and streams the matching
// documents.
func (c *Client) Find(ctx context.Context, collection string, req *FindRequest) (*JSONStream, error) {
	body, err := encodeQueryRequest(req)
	if err != nil {
		return nil, fmt.Errorf("encode find request: %w", err)
	}
	return c.stream(ctx, http.MethodPost, collectionActionPath(collection, "find"), body, "application/json")
}

// Patch applies a partial update to the documents matched by the query and
// streams each patched document.
func (c *Client) Patch(ctx context.Context, collection string, req *PatchRequest) (*JSONStream, error) {
	if req == nil {
		return nil, errors.New("patch request is nil")
	}
	body, err := encodeQueryRequest(req)
	if err != nil {
		return nil, fmt.Errorf("encode patch request: %w", err)
	}
	return c.stream(ctx, http.MethodPost, collectionActionPath(collection, "patch"), body, "application/json")
}

// Remove deletes the documents matched by the query and streams the removed
// documents back to the caller.
func (c *Client) Remove(ctx context.Context, collection string, req *RemoveRequest) (*JSONStream, error) {
	body, err := encodeQueryRequest(req)
	if err != nil {
		return nil, fmt.Errorf("encode remove request: %w", err)
	}
	return c.stream(ctx, http.MethodPost, collectionActionPath(collection, "remove"), body, "application/json")
}

func (c *Client) stream(ctx context.Context, method, path string, body io.Reader, contentType string) (*JSONStream, error) {
	resp, err := c.do(ctx, method, path, body, contentType)
	if err != nil {
		return nil, err
	}
	return newJSONStream(resp), nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body io.Reader, dest any) error {
	resp, err := c.do(ctx, method, path, body, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if dest == nil || resp.StatusCode == http.StatusNoContent {
		io.Copy(io.Discard, resp.Body)
		return nil
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	rel, err := url.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path %q: %w", path, err)
	}
	endpoint := c.baseURL.ResolveReference(rel)

	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}

	if contentType == "" && body != nil {
		contentType = "application/json"
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, parseErrorResponse(resp)
	}

	return resp, nil
}

func collectionPath(collection string) string {
	return "/v1/collections/" + url.PathEscape(collection)
}

func collectionActionPath(collection, action string) string {
	return collectionPath(collection) + ":" + action
}

func encodeJSONPayload(payload any) (io.Reader, error) {
	if payload == nil {
		return nil, nil
	}
	switch v := payload.(type) {
	case io.Reader:
		return v, nil
	case []byte:
		data := make([]byte, len(v))
		copy(data, v)
		return bytes.NewReader(data), nil
	case string:
		return strings.NewReader(v), nil
	case json.RawMessage:
		data := make([]byte, len(v))
		copy(data, v)
		return bytes.NewReader(data), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	}
}

func encodeJSONObject(payload any) (io.Reader, error) {
	switch v := payload.(type) {
	case nil:
		return bytes.NewReader([]byte("{}")), nil
	case io.Reader:
		return v, nil
	case []byte:
		trimmed := bytes.TrimSpace(v)
		if len(trimmed) == 0 {
			return bytes.NewReader([]byte("{}")), nil
		}
		data := make([]byte, len(v))
		copy(data, v)
		return bytes.NewReader(data), nil
	case string:
		if strings.TrimSpace(v) == "" {
			return bytes.NewReader([]byte("{}")), nil
		}
		return strings.NewReader(v), nil
	case json.RawMessage:
		return encodeJSONObject([]byte(v))
	default:
		val := reflect.ValueOf(payload)
		switch val.Kind() { //nolint:exhaustive // interested in composite types.
		case reflect.Map:
			if val.IsNil() {
				return bytes.NewReader([]byte("{}")), nil
			}
		case reflect.Pointer, reflect.Interface:
			if val.IsNil() {
				return bytes.NewReader([]byte("{}")), nil
			}
		}
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	}
}

func encodeJSONLines(items []any) (io.Reader, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	for _, item := range items {
		if err := enc.Encode(item); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeQueryRequest(payload any) (io.Reader, error) {
	if payload == nil {
		return bytes.NewReader([]byte("{}")), nil
	}
	val := reflect.ValueOf(payload)
	switch val.Kind() { //nolint:exhaustive // only care about pointer-like kinds.
	case reflect.Pointer, reflect.Interface:
		if val.IsNil() {
			return bytes.NewReader([]byte("{}")), nil
		}
	}
	return encodeJSONObject(payload)
}
