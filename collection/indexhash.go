package collection

import (
	"encoding/json"
	"fmt"
	"sync"
)

type IndexHashOptions struct {
	Field string `json:"field"`
}

type IndexHashTraverse struct {
	Value string `json:"value"`
}

type IndexHash struct {
	options *IndexHashOptions
	entries map[string][]*Row
	mu      sync.RWMutex
}

func NewIndexHash(options *IndexHashOptions) *IndexHash {
	return &IndexHash{
		options: options,
		entries: map[string][]*Row{},
	}
}

func (i *IndexHash) AddRow(row *Row) error {
	item := map[string]interface{}{}
	if err := json.Unmarshal(row.Payload, &item); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	field := i.options.Field
	value, exists := item[field]
	if !exists {
		return nil
	}

	switch v := value.(type) {
	case string:
		i.mu.Lock()
		i.entries[v] = append(i.entries[v], row)
		i.mu.Unlock()
	case []interface{}:
		i.mu.Lock()
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				i.mu.Unlock()
				return fmt.Errorf("type not supported")
			}
			i.entries[s] = append(i.entries[s], row)
		}
		i.mu.Unlock()
	default:
		return fmt.Errorf("type not supported")
	}

	return nil
}

func (i *IndexHash) RemoveRow(row *Row) error {
	item := map[string]interface{}{}
	if err := json.Unmarshal(row.Payload, &item); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	field := i.options.Field
	value, exists := item[field]
	if !exists {
		return nil
	}

	removeRow := func(key string) {
		slice := i.entries[key]
		for idx, candidate := range slice {
			if candidate == row {
				slice = append(slice[:idx], slice[idx+1:]...)
				break
			}
		}
		if len(slice) == 0 {
			delete(i.entries, key)
		} else {
			i.entries[key] = slice
		}
	}

	switch v := value.(type) {
	case string:
		i.mu.Lock()
		removeRow(v)
		i.mu.Unlock()
	case []interface{}:
		i.mu.Lock()
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				i.mu.Unlock()
				return fmt.Errorf("type not supported")
			}
			removeRow(s)
		}
		i.mu.Unlock()
	default:
		return fmt.Errorf("type not supported")
	}

	return nil
}

func (i *IndexHash) Traverse(optionsData []byte, f func(row *Row) bool) {
	traverseOptions := &IndexHashTraverse{}
	json.Unmarshal(optionsData, traverseOptions) // todo: handle error

	i.mu.RLock()
	rows := append([]*Row(nil), i.entries[traverseOptions.Value]...)
	i.mu.RUnlock()

	for _, row := range rows {
		if !f(row) {
			break
		}
	}
}

func init() {
	MustRegisterIndex(&IndexDefinition{
		Type: "hash",
		NewOptions: func() interface{} {
			return &IndexHashOptions{}
		},
		Builder: func(options interface{}) (Index, error) {
			opts, ok := options.(*IndexHashOptions)
			if !ok {
				return nil, fmt.Errorf("invalid options type %T for hash index", options)
			}
			if opts.Field == "" {
				return nil, fmt.Errorf("field is required")
			}
			return NewIndexHash(opts), nil
		},
	})
}
