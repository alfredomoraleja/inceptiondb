package driver

import "encoding/json"

// Collection describes a collection metadata entry returned by the API.
type Collection struct {
	Name     string         `json:"name"`
	Total    int            `json:"total"`
	Indexes  int            `json:"indexes"`
	Defaults map[string]any `json:"defaults,omitempty"`
}

// CreateCollectionRequest contains the payload required to create a
// collection.
type CreateCollectionRequest struct {
	Name     string         `json:"name"`
	Defaults map[string]any `json:"defaults,omitempty"`
}

// Index describes an index configuration as returned by the API. Dynamic
// options are stored inside Options.
type Index struct {
	Name    string         `json:"name"`
	Type    string         `json:"type"`
	Options map[string]any `json:"-"`
}

// MarshalJSON flattens the index options so the payload matches the HTTP API.
func (i Index) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(i.Options)+2)
	m["name"] = i.Name
	m["type"] = i.Type
	for k, v := range i.Options {
		m[k] = v
	}
	return json.Marshal(m)
}

// UnmarshalJSON extracts the name and type fields while keeping the rest of the
// payload inside Options.
func (i *Index) UnmarshalJSON(data []byte) error {
	raw := map[string]any{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if v, ok := raw["name"].(string); ok {
		i.Name = v
	}
	if v, ok := raw["type"].(string); ok {
		i.Type = v
	}
	delete(raw, "name")
	delete(raw, "type")
	if len(raw) == 0 {
		i.Options = nil
	} else {
		i.Options = raw
	}
	return nil
}

// CreateIndexRequest captures the parameters used to create an index. The
// Options map is flattened when marshalled so it matches the HTTP API format.
type CreateIndexRequest struct {
	Name    string         `json:"name"`
	Type    string         `json:"type"`
	Options map[string]any `json:"options,omitempty"`
}

// MarshalJSON flattens Options to the top-level keys expected by the API.
func (r CreateIndexRequest) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(r.Options)+2)
	if r.Name != "" {
		m["name"] = r.Name
	}
	if r.Type != "" {
		m["type"] = r.Type
	}
	for k, v := range r.Options {
		m[k] = v
	}
	return json.Marshal(m)
}

// QueryOptions represents the shared query parameters used by find, patch and
// remove operations.
type QueryOptions struct {
	Mode    string         `json:"mode,omitempty"`
	Index   string         `json:"index,omitempty"`
	Filter  map[string]any `json:"filter,omitempty"`
	Skip    int64          `json:"skip,omitempty"`
	Limit   int64          `json:"limit,omitempty"`
	Reverse bool           `json:"reverse,omitempty"`
	From    map[string]any `json:"from,omitempty"`
	To      map[string]any `json:"to,omitempty"`
	Value   string         `json:"value,omitempty"`
}

// FindRequest captures the options available when querying documents.
type FindRequest struct {
	QueryOptions
}

// PatchRequest describes the payload required to patch documents.
type PatchRequest struct {
	QueryOptions
	Patch any `json:"patch"`
}

// RemoveRequest contains the parameters accepted by the remove endpoint.
type RemoveRequest struct {
	QueryOptions
}
