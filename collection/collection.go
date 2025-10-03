package collection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/google/uuid"

	"github.com/fulldump/inceptiondb/utils"
)

type Collection struct {
	Filename string // Just informative...
	file     *os.File

	Rows   []*Row
	rowsMu sync.RWMutex

	Indexes   map[string]*collectionIndex // todo: protect access with mutex or use sync.Map
	indexesMu sync.RWMutex

	// buffer   *bufio.Writer // TODO: use write buffer to improve performance (x3 in tests)
	Defaults map[string]any
	Count    int64

	writeMu sync.Mutex
}

type indexEntry struct {
	name  string
	index *collectionIndex
}

var commandBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

func (c *Collection) withIndexes(f func([]indexEntry) error) error {
	c.indexesMu.RLock()
	defer c.indexesMu.RUnlock()

	if len(c.Indexes) == 0 {
		return f(nil)
	}

	indexes := make([]indexEntry, 0, len(c.Indexes))
	for name, idx := range c.Indexes {
		indexes = append(indexes, indexEntry{name: name, index: idx})
	}

	return f(indexes)
}

func (c *Collection) writeCommand(command *Command) error {
	if c.file == nil {
		return fmt.Errorf("collection is closed")
	}

	buf := commandBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer commandBufferPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(command); err != nil {
		return fmt.Errorf("json encode command: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write command: %w", err)
	}

	return nil
}

type collectionIndex struct {
	Index
	Type    string
	Options interface{}
}

type Row struct {
	I          int // position in Rows
	Payload    json.RawMessage
	PatchMutex sync.Mutex
}

func OpenCollection(filename string) (*Collection, error) {

	// TODO: initialize, read all file and apply its changes into memory
	f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file for read: %w", err)
	}

	collection := &Collection{
		Rows:     []*Row{},
		Filename: filename,
		Indexes:  map[string]*collectionIndex{},
	}

	j := jsontext.NewDecoder(f)
	for {
		command := &Command{}
		err := jsonv2.UnmarshalDecode(j, &command)
		if err == io.EOF {
			break
		}
		if err != nil {
			// todo: try a best effort?
			return nil, fmt.Errorf("decode json: %w", err)
		}

		switch command.Name {
		case "insert":
			_, err := collection.addRow(command.Payload)
			if err != nil {
				return nil, err
			}
		case "drop_index":
			dropIndexCommand := &DropIndexCommand{}
			jsonv2.Unmarshal(command.Payload, dropIndexCommand) // Todo: handle error properly

			err := collection.dropIndex(dropIndexCommand.Name, false)
			if err != nil {
				fmt.Printf("WARNING: drop index '%s': %s\n", dropIndexCommand.Name, err.Error())
				// TODO: stop process? if error might get inconsistent state
			}
		case "index": // todo: rename to create_index
			indexCommand := &CreateIndexCommand{}
			jsonv2.Unmarshal(command.Payload, indexCommand) // Todo: handle error properly

			var options interface{}

			switch indexCommand.Type {
			case "map":
				options = &IndexMapOptions{}
				utils.Remarshal(indexCommand.Options, options)
			case "btree":
				options = &IndexBTreeOptions{}
				utils.Remarshal(indexCommand.Options, options)
			default:
				return nil, fmt.Errorf("index command: unexpected type '%s' instead of [map|btree]", indexCommand.Type)
			}

			err := collection.createIndex(indexCommand.Name, options, false)
			if err != nil {
				fmt.Printf("WARNING: create index '%s': %s\n", indexCommand.Name, err.Error())
			}
		case "remove":
			params := struct {
				I int `json:"i"`
			}{}
			jsonv2.Unmarshal(command.Payload, &params) // Todo: handle error properly
			row := collection.Rows[params.I]           // this access is threadsafe, OpenCollection is a secuence
			err := collection.removeByRow(row, false)
			if err != nil {
				fmt.Printf("WARNING: remove row %d: %s\n", params.I, err.Error())
			}
		case "patch":
			params := struct {
				I    int                    `json:"i"`
				Diff map[string]interface{} `json:"diff"`
			}{}
			jsonv2.Unmarshal(command.Payload, &params)
			row := collection.Rows[params.I] // this access is threadsafe, OpenCollection is a secuence
			err := collection.patchByRow(row, params.Diff, false)
			if err != nil {
				fmt.Printf("WARNING: patch item %d: %s\n", params.I, err.Error())
			}
		case "set_defaults":
			defaults := map[string]any{}
			jsonv2.Unmarshal(command.Payload, &defaults)
			collection.setDefaults(defaults, false)
		}
	}

	// Open file for append only
	// todo: investigate O_SYNC
	collection.file, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file for write: %w", err)
	}

	return collection, nil
}

func (c *Collection) addRow(payload json.RawMessage) (*Row, error) {

	row := &Row{
		Payload: payload,
	}

	err := c.withIndexes(func(indexes []indexEntry) error {
		return indexInsert(indexes, row)
	})
	if err != nil {
		return nil, err
	}

	c.rowsMu.Lock()
	row.I = len(c.Rows)
	c.Rows = append(c.Rows, row)
	c.rowsMu.Unlock()

	return row, nil
}

// TODO: test concurrency
func (c *Collection) Insert(item interface{}) (*Row, error) {
	if c.file == nil {
		return nil, fmt.Errorf("collection is closed")
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("json encode payload: %w", err)
	}

	auto := atomic.AddInt64(&c.Count, 1)

	if c.Defaults != nil {
		item := map[string]any{} // todo: item is shadowed, choose a better name
		for k, v := range c.Defaults {
			switch v {
			case "uuid()":
				item[k] = uuid.NewString()
			case "unixnano()":
				item[k] = time.Now().UnixNano()
			case "auto()":
				item[k] = auto
			default:
				item[k] = v
			}
		}
		err := jsonv2.Unmarshal(payload, &item)
		if err != nil {
			return nil, fmt.Errorf("json encode defaults: %w", err)
		}

		payload, err = json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("json encode payload: %w", err)
		}
	}

	// Add row
	row, err := c.addRow(payload)
	if err != nil {
		return nil, err
	}

	// Persist
	command := &Command{
		Name:      "insert",
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (c *Collection) FindOne(data interface{}) {
	c.rowsMu.RLock()
	defer c.rowsMu.RUnlock()

	for _, row := range c.Rows {
		jsonv2.Unmarshal(row.Payload, data)
		return
	}
	// TODO return with error not found? or nil?
}

func (c *Collection) Traverse(f func(data []byte)) { // todo: return *Row instead of data?
	c.rowsMu.RLock()
	defer c.rowsMu.RUnlock()

	for _, row := range c.Rows {
		f(row.Payload)
	}
}

func (c *Collection) TraverseRange(from, to int, f func(row *Row)) { // todo: improve this naive  implementation
	c.rowsMu.RLock()
	defer c.rowsMu.RUnlock()

	for i, row := range c.Rows {
		if i < from {
			continue
		}
		if to > 0 && i >= to {
			break
		}
		f(row)
	}
}

type CreateIndexOptions struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Options interface{} `json:"options"`
}

type CreateIndexCommand struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Options interface{} `json:"options"`
}

func (c *Collection) SetDefaults(defaults map[string]any) error {
	return c.setDefaults(defaults, true)
}

func (c *Collection) setDefaults(defaults map[string]any, persist bool) error {

	c.Defaults = defaults

	if !persist {
		return nil
	}

	payload, err := json.Marshal(defaults)
	if err != nil {
		return fmt.Errorf("json encode payload: %w", err)
	}

	command := &Command{
		Name:      "set_defaults", // todo: rename to create_index
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return err
	}

	return nil
}

// IndexMap create a unique index with a name
// Constraints: values can be only scalar strings or array of strings
func (c *Collection) Index(name string, options interface{}) error { // todo: rename to CreateIndex
	return c.createIndex(name, options, true)
}

func (c *Collection) createIndex(name string, options interface{}, persist bool) error {

	c.indexesMu.RLock()
	_, exists := c.Indexes[name]
	c.indexesMu.RUnlock()
	if exists {
		return fmt.Errorf("index '%s' already exists", name)
	}

	index := &collectionIndex{}

	switch value := options.(type) {
	case *IndexMapOptions:
		index.Type = "map"
		index.Index = NewIndexSyncMap(value)
		index.Options = value
	case *IndexBTreeOptions:
		index.Type = "btree"
		index.Index = NewIndexBTree(value)
		index.Options = value
	default:
		return fmt.Errorf("unexpected options parameters, it should be [map|btree]")
	}

	c.rowsMu.RLock()
	defer c.rowsMu.RUnlock()
	for _, row := range c.Rows {
		err := index.AddRow(row)
		if err != nil {
			return fmt.Errorf("index row: %s, data: %s", err.Error(), string(row.Payload))
		}
	}

	c.indexesMu.Lock()
	if _, exists := c.Indexes[name]; exists {
		c.indexesMu.Unlock()
		return fmt.Errorf("index '%s' already exists", name)
	}
	c.Indexes[name] = index
	c.indexesMu.Unlock()

	if !persist {
		return nil
	}

	payload, err := json.Marshal(&CreateIndexCommand{
		Name:    name,
		Type:    index.Type,
		Options: options,
	})
	if err != nil {
		return fmt.Errorf("json encode payload: %w", err)
	}

	command := &Command{
		Name:      "index", // todo: rename to create_index
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return err
	}

	return nil
}

func indexInsert(indexes []indexEntry, row *Row) (err error) {

	// Note: rollbacks array should be kept in stack if it is smaller than 65536 bytes, so
	// our recommended maximum number of indexes should NOT exceed 8192 indexes

	rollbacks := make([]*collectionIndex, len(indexes))
	c := 0

	defer func() {
		if err == nil {
			return
		}
		for i := 0; i < c; i++ {
			rollbacks[i].RemoveRow(row)
		}
	}()

	for _, entry := range indexes {
		err = entry.index.AddRow(row)
		if err != nil {
			return fmt.Errorf("index add '%s': %s", entry.name, err.Error())
		}

		rollbacks[c] = entry.index
		c++
	}

	return
}

func indexRemove(indexes []indexEntry, row *Row) (err error) {
	for _, entry := range indexes {
		err = entry.index.RemoveRow(row)
		if err != nil {
			// TODO: does this make any sense?
			return fmt.Errorf("index remove '%s': %s", entry.name, err.Error())
		}
	}

	return
}

func (c *Collection) Remove(r *Row) error {
	return c.removeByRow(r, true)
}

// TODO: move this to utils/diogenesis?
func lockBlock(lock sync.Locker, f func() error) error {
	lock.Lock()
	defer lock.Unlock()
	return f()
}

func (c *Collection) removeByRow(row *Row, persist bool) error { // todo: rename to 'removeRow'

	var i int
	err := lockBlock(&c.rowsMu, func() error {
		i = row.I
		if len(c.Rows) <= i {
			return fmt.Errorf("row %d does not exist", i)
		}

		err := c.withIndexes(func(indexes []indexEntry) error {
			return indexRemove(indexes, row)
		})
		if err != nil {
			return fmt.Errorf("could not free index: %w", err)
		}

		last := len(c.Rows) - 1
		c.Rows[i] = c.Rows[last]
		c.Rows[i].I = i
		c.Rows = c.Rows[:last]
		return nil
	})
	if err != nil {
		return err
	}

	if !persist {
		return nil
	}

	// Persist
	payload, err := json.Marshal(map[string]interface{}{
		"i": i,
	})
	if err != nil {
		return err // todo: wrap error
	}
	command := &Command{
		Name:      "remove",
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) Patch(row *Row, patch interface{}) error {
	return c.patchByRow(row, patch, true)
}

func (c *Collection) patchByRow(row *Row, patch interface{}, persist bool) error { // todo: rename to 'patchRow'

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	newPayload, err := jsonpatch.MergePatch(row.Payload, patchBytes)
	if err != nil {
		return fmt.Errorf("cannot apply patch: %w", err)
	}

	diff, err := jsonpatch.CreateMergePatch(row.Payload, newPayload) // todo: optimization: discard operation if empty
	if err != nil {
		return fmt.Errorf("cannot diff: %w", err)
	}

	if len(diff) == 2 { // diff == '{}'
		return nil
	}

	// index update
	err = c.withIndexes(func(indexes []indexEntry) error {
		if err := indexRemove(indexes, row); err != nil {
			return fmt.Errorf("indexRemove: %w", err)
		}

		row.Payload = newPayload

		if err := indexInsert(indexes, row); err != nil {
			return fmt.Errorf("indexInsert: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !persist {
		return nil
	}

	// Persist
	payload, err := json.Marshal(map[string]interface{}{
		"i":    row.I,
		"diff": json.RawMessage(diff),
	})
	if err != nil {
		return err // todo: wrap error
	}
	command := &Command{
		Name:      "patch",
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) Close() error {
	err := c.file.Close()
	c.file = nil
	return err
}

func (c *Collection) Drop() error {
	err := c.Close()
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}

	err = os.Remove(c.Filename)
	if err != nil {
		return fmt.Errorf("remove: %w", err)
	}

	return nil
}

func (c *Collection) DropIndex(name string) error {
	return c.dropIndex(name, true)
}

type DropIndexCommand struct {
	Name string `json:"name"`
}

func (c *Collection) dropIndex(name string, persist bool) error {
	c.indexesMu.Lock()
	_, exists := c.Indexes[name]
	if !exists {
		c.indexesMu.Unlock()
		return fmt.Errorf("dropIndex: index '%s' not found", name)
	}
	delete(c.Indexes, name)
	c.indexesMu.Unlock()

	if !persist {
		return nil
	}

	payload, err := json.Marshal(&CreateIndexCommand{
		Name: name,
	})
	if err != nil {
		return fmt.Errorf("json encode payload: %w", err)
	}

	command := &Command{
		Name:      "drop_index",
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   payload,
	}

	err = c.writeCommand(command)
	if err != nil {
		return err
	}

	return nil
}
