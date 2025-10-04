package collection

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
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
	Filename  string // Just informative...
	file      *os.File
	Rows      []*Row
	rowsMutex sync.RWMutex
	Indexes   map[string]*collectionIndex // todo: protect access with mutex or use sync.Map
	// buffer   *bufio.Writer // TODO: use write buffer to improve performance (x3 in tests)
	Defaults map[string]any
	Count    int64

	persistQueue chan *persistRequest
	persistWG    sync.WaitGroup
	persistErr   atomic.Pointer[error]
	closeOnce    sync.Once
	closing      atomic.Bool
}

type collectionIndex struct {
	Index
	Type    string
	Options interface{}
}

const commandQueueSize = 1024

type persistRequest struct {
	command *Command
	result  chan error
}

var commandBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type Row struct {
	I          int // position in Rows
	Payload    stdjson.RawMessage
	PatchMutex sync.Mutex
}

func (c *Collection) startPersistence() {
	c.persistQueue = make(chan *persistRequest, commandQueueSize)
	c.persistWG.Add(1)
	go c.persistLoop()
}

func (c *Collection) persistLoop() {
	defer c.persistWG.Done()

	for request := range c.persistQueue {
		if request == nil {
			continue
		}

		var err error

		buffer := commandBufferPool.Get().(*bytes.Buffer)
		buffer.Reset()

		if encodeErr := stdjson.NewEncoder(buffer).Encode(request.command); encodeErr != nil {
			err = fmt.Errorf("encode command: %w", encodeErr)
		} else if c.file != nil {
			if _, writeErr := c.file.Write(buffer.Bytes()); writeErr != nil {
				err = fmt.Errorf("write command: %w", writeErr)
			}
		} else {
			err = fmt.Errorf("write command: collection file is nil")
		}

		commandBufferPool.Put(buffer)

		if err != nil {
			c.setPersistError(err)
		}

		if request.result != nil {
			request.result <- err
		}
	}
}

func (c *Collection) enqueueCommand(command *Command) error {
	if c.closing.Load() {
		return fmt.Errorf("collection is closing")
	}

	if err := c.persistError(); err != nil {
		return fmt.Errorf("persist command: %w", err)
	}

	request := &persistRequest{
		command: command,
		result:  make(chan error, 1),
	}

	c.persistQueue <- request

	if err := <-request.result; err != nil {
		return fmt.Errorf("persist command: %w", err)
	}

	return nil
}

func (c *Collection) persistError() error {
	if ptr := c.persistErr.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

func (c *Collection) setPersistError(err error) {
	if err == nil {
		return
	}
	for {
		if current := c.persistErr.Load(); current != nil {
			return
		}
		ptr := new(error)
		*ptr = err
		if c.persistErr.CompareAndSwap(nil, ptr) {
			return
		}
	}
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

	decoder := jsontext.NewDecoder(f)
	for {
		command := &Command{}
		err := jsonv2.UnmarshalDecode(decoder, command)
		if errors.Is(err, io.EOF) {
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

	collection.startPersistence()

	return collection, nil
}

func (c *Collection) addRow(payload stdjson.RawMessage) (*Row, error) {

	row := &Row{
		Payload: payload,
	}

	err := indexInsert(c.Indexes, row)
	if err != nil {
		return nil, err
	}

	c.rowsMutex.Lock()
	row.I = len(c.Rows)
	c.Rows = append(c.Rows, row)
	c.rowsMutex.Unlock()

	return row, nil
}

// TODO: test concurrency
func (c *Collection) Insert(item interface{}) (*Row, error) {
	if c.file == nil {
		return nil, fmt.Errorf("collection is closed")
	}

	encodedPayload, err := stdjson.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("json encode payload: %w", err)
	}

	payload := stdjson.RawMessage(encodedPayload)

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

		rawPayload, err := stdjson.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("json encode payload: %w", err)
		}
		payload = stdjson.RawMessage(rawPayload)
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

	err = c.enqueueCommand(command)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (c *Collection) FindOne(data interface{}) {
	c.rowsMutex.RLock()
	defer c.rowsMutex.RUnlock()

	for _, row := range c.Rows {
		jsonv2.Unmarshal(row.Payload, data)
		return
	}
	// TODO return with error not found? or nil?
}

func (c *Collection) Traverse(f func(data []byte)) { // todo: return *Row instead of data?
	c.rowsMutex.RLock()
	rows := append([]*Row(nil), c.Rows...)
	c.rowsMutex.RUnlock()

	for _, row := range rows {
		f(row.Payload)
	}
}

func (c *Collection) TraverseRange(from, to int, f func(row *Row)) { // todo: improve this naive  implementation
	c.rowsMutex.RLock()
	rows := append([]*Row(nil), c.Rows...)
	c.rowsMutex.RUnlock()

	for i, row := range rows {
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

	payloadBytes, err := stdjson.Marshal(defaults)
	if err != nil {
		return fmt.Errorf("json encode payload: %w", err)
	}

	command := &Command{
		Name:      "set_defaults", // todo: rename to create_index
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   stdjson.RawMessage(payloadBytes),
	}

	return c.enqueueCommand(command)
}

// IndexMap create a unique index with a name
// Constraints: values can be only scalar strings or array of strings
func (c *Collection) Index(name string, options interface{}) error { // todo: rename to CreateIndex
	return c.createIndex(name, options, true)
}

func (c *Collection) createIndex(name string, options interface{}, persist bool) error {

	if _, exists := c.Indexes[name]; exists {
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

	c.Indexes[name] = index

	c.rowsMutex.RLock()
	rows := append([]*Row(nil), c.Rows...)
	c.rowsMutex.RUnlock()

	// Add all rows to the index
	for _, row := range rows {
		err := index.AddRow(row)
		if err != nil {
			delete(c.Indexes, name)
			return fmt.Errorf("index row: %s, data: %s", err.Error(), string(row.Payload))
		}
	}

	if !persist {
		return nil
	}

	payloadBytes, err := stdjson.Marshal(&CreateIndexCommand{
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
		Payload:   stdjson.RawMessage(payloadBytes),
	}

	return c.enqueueCommand(command)
}

func indexInsert(indexes map[string]*collectionIndex, row *Row) (err error) {

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

	for key, index := range indexes {
		err = index.AddRow(row)
		if err != nil {
			return fmt.Errorf("index add '%s': %s", key, err.Error())
		}

		rollbacks[c] = index
		c++
	}

	return
}

func indexRemove(indexes map[string]*collectionIndex, row *Row) (err error) {
	for key, index := range indexes {
		err = index.RemoveRow(row)
		if err != nil {
			// TODO: does this make any sense?
			return fmt.Errorf("index remove '%s': %s", key, err.Error())
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
	err := lockBlock(&c.rowsMutex, func() error {
		i = row.I
		if len(c.Rows) <= i {
			return fmt.Errorf("row %d does not exist", i)
		}

		err := indexRemove(c.Indexes, row)
		if err != nil {
			return fmt.Errorf("could not free index")
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
	payloadBytes, err := stdjson.Marshal(map[string]interface{}{
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
		Payload:   stdjson.RawMessage(payloadBytes),
	}

	return c.enqueueCommand(command)
}

func (c *Collection) Patch(row *Row, patch interface{}) error {
	return c.patchByRow(row, patch, true)
}

func (c *Collection) patchByRow(row *Row, patch interface{}, persist bool) error { // todo: rename to 'patchRow'

	patchBytes, err := stdjson.Marshal(patch)
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
	err = indexRemove(c.Indexes, row)
	if err != nil {
		return fmt.Errorf("indexRemove: %w", err)
	}

	row.Payload = newPayload

	err = indexInsert(c.Indexes, row)
	if err != nil {
		return fmt.Errorf("indexInsert: %w", err)
	}

	if !persist {
		return nil
	}

	// Persist
	payloadBytes, err := stdjson.Marshal(map[string]interface{}{
		"i":    row.I,
		"diff": stdjson.RawMessage(diff),
	})
	if err != nil {
		return err // todo: wrap error
	}
	command := &Command{
		Name:      "patch",
		Uuid:      uuid.New().String(),
		Timestamp: time.Now().UnixNano(),
		StartByte: 0,
		Payload:   stdjson.RawMessage(payloadBytes),
	}

	return c.enqueueCommand(command)
}

func (c *Collection) Close() error {
	var err error

	c.closeOnce.Do(func() {
		c.closing.Store(true)

		if c.persistQueue != nil {
			close(c.persistQueue)
			c.persistWG.Wait()
		}

		if c.file != nil {
			err = c.file.Close()
			c.file = nil
		}
	})

	if persistErr := c.persistError(); persistErr != nil && err == nil {
		err = persistErr
	}

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
	_, exists := c.Indexes[name]
	if !exists {
		return fmt.Errorf("dropIndex: index '%s' not found", name)
	}
	delete(c.Indexes, name)

	if !persist {
		return nil
	}

	payloadBytes, err := stdjson.Marshal(&CreateIndexCommand{
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
		Payload:   stdjson.RawMessage(payloadBytes),
	}

	return c.enqueueCommand(command)
}
