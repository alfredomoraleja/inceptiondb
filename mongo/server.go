package mongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SierraSoftworks/connor"

	"github.com/fulldump/inceptiondb/service"
)

type Server struct {
	svc       service.Servicer
	listener  net.Listener
	wg        sync.WaitGroup
	closeOnce sync.Once
	stopChan  chan struct{}
}

func NewServer(s service.Servicer) *Server {
	return &Server{
		svc:      s,
		stopChan: make(chan struct{}),
	}
}

func (s *Server) Serve(l net.Listener) error {
	s.listener = l
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("mongo: temporary accept error: %v", err)
				continue
			}
			select {
			case <-s.stopChan:
				return nil
			default:
				return err
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stopChan)
		if s.listener != nil {
			err = s.listener.Close()
		}
	})
	s.wg.Wait()
	return err
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := readMessage(conn)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				log.Printf("mongo: read message error: %v", err)
			}
			return
		}
		response, err := s.handleCommand(msg.body)
		if err != nil {
			if writeErr := writeMessage(conn, msg.header.RequestID, NewDocument(
				"ok", float64(0),
				"errmsg", err.Error(),
			)); writeErr != nil {
				log.Printf("mongo: write error: %v", writeErr)
				return
			}
			continue
		}
		var writeErr error
		switch msg.header.OpCode {
		case opMsg:
			writeErr = writeMessage(conn, msg.header.RequestID, response)
		case opQuery:
			writeErr = writeReply(conn, msg.header.RequestID, response)
		default:
			writeErr = fmt.Errorf("unsupported response opcode %d", msg.header.OpCode)
		}
		if writeErr != nil {
			log.Printf("mongo: write message error: %v", writeErr)
			return
		}
	}
}

func (s *Server) handleCommand(doc Document) (*Document, error) {
	commandName, _, ok := doc.FirstKey()
	if !ok {
		return NewDocument("ok", float64(0), "errmsg", "empty command"), nil
	}
	switch commandName {
	case "isMaster", "ismaster", "hello":
		return s.handleHello(doc), nil
	case "buildInfo":
		return s.handleBuildInfo(), nil
	case "ping":
		return NewDocument("ok", float64(1)), nil
	case "listDatabases":
		return s.handleListDatabases(), nil
	case "listCollections":
		return s.handleListCollections(doc)
	case "insert":
		return s.handleInsert(doc)
	case "find":
		return s.handleFind(doc)
	default:
		return NewDocument(
			"ok", float64(0),
			"errmsg", fmt.Sprintf("command %s not supported", commandName),
		), nil
	}
}

func (s *Server) handleHello(doc Document) *Document {
	return NewDocument(
		"ismaster", true,
		"isWritablePrimary", true,
		"maxBsonObjectSize", int32(16*1024*1024),
		"maxMessageSizeBytes", int32(48*1024*1024),
		"maxWriteBatchSize", int32(100000),
		"minWireVersion", int32(0),
		"maxWireVersion", int32(13),
		"localTime", time.Now().UTC(),
		"logicalSessionTimeoutMinutes", int32(30),
		"connectionId", int32(1),
		"helloOk", true,
		"ok", float64(1),
	)
}

func (s *Server) handleBuildInfo() *Document {
	return NewDocument(
		"version", "0.0.1",
		"gitVersion", "inceptiondb",
		"modules", NewArray(),
		"sysInfo", "",
		"ok", float64(1),
	)
}

func (s *Server) handleListDatabases() *Document {
	collections := s.svc.ListCollections()
	dbSet := map[string]struct{}{}
	for name := range collections {
		dbName, _ := splitNamespace(name)
		dbSet[dbName] = struct{}{}
	}
	if len(dbSet) == 0 {
		dbSet["default"] = struct{}{}
	}
	dbNames := make([]string, 0, len(dbSet))
	for name := range dbSet {
		dbNames = append(dbNames, name)
	}
	sort.Strings(dbNames)
	databases := make([]interface{}, 0, len(dbNames))
	for _, name := range dbNames {
		databases = append(databases, NewDocument(
			"name", name,
			"sizeOnDisk", float64(1),
			"empty", false,
		))
	}
	return NewDocument(
		"databases", NewArray(databases...),
		"totalSize", float64(1),
		"ok", float64(1),
	)
}

func (s *Server) handleListCollections(doc Document) (*Document, error) {
	dbName, _ := s.extractDatabase(doc)
	collections := s.svc.ListCollections()
	entries := []interface{}{}
	for name, col := range collections {
		currentDB, coll := splitNamespace(name)
		if dbName != "" && currentDB != dbName {
			continue
		}
		options := ensureDocument(col.Defaults)
		entry := NewDocument(
			"name", coll,
			"type", "collection",
			"options", options,
			"info", NewDocument("readOnly", false, "uuid", fmt.Sprintf("%s:%s", currentDB, coll)),
			"idIndex", NewDocument(),
		)
		entries = append(entries, entry)
	}
	ns := fmt.Sprintf("%s.$cmd.listCollections", dbName)
	cursor := NewDocument(
		"firstBatch", NewArray(entries...),
		"id", int64(0),
		"ns", ns,
	)
	return NewDocument("cursor", cursor, "ok", float64(1)), nil
}

func (s *Server) handleInsert(doc Document) (*Document, error) {
	collectionValue, ok := doc.Get("insert")
	if !ok {
		return nil, fmt.Errorf("missing collection in insert command")
	}
	collectionName, ok := collectionValue.(string)
	if !ok {
		return nil, fmt.Errorf("collection name must be string")
	}
	// dbName, _ := s.extractDatabase(doc)
	documentsValue, ok := doc.Get("documents")
	if !ok {
		return nil, fmt.Errorf("missing documents array")
	}
	documents, ok := documentsValue.(Array)
	if !ok {
		return nil, fmt.Errorf("documents must be array")
	}
	// fullName := buildNamespace(dbName, collectionName)
	col, err := s.svc.GetCollection(collectionName)
	if err == service.ErrorCollectionNotFound {
		col, err = s.svc.CreateCollection(collectionName)
		if err != nil {
			return nil, err
		}
		if err := col.SetDefaults(map[string]any{"_id": "uuid()"}); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	inserted := 0
	for _, raw := range documents {
		docValue, ok := raw.(*Document)
		if !ok {
			return nil, fmt.Errorf("document must be bson document")
		}
		payload := docValue.ToMap()
		if _, exists := payload["_id"]; !exists {
			payload["_id"] = nil
		}
		_, err = col.Insert(payload)
		if err != nil {
			return nil, err
		}
		inserted++
	}
	return NewDocument(
		"n", int32(inserted),
		"ok", float64(1),
	), nil
}

func (s *Server) handleFind(doc Document) (*Document, error) {
	collectionValue, ok := doc.Get("find")
	if !ok {
		return nil, fmt.Errorf("missing collection in find command")
	}
	collectionName, ok := collectionValue.(string)
	if !ok {
		return nil, fmt.Errorf("collection name must be string")
	}
	dbName, _ := s.extractDatabase(doc)
	// fullName := buildNamespace(dbName, collectionName)
	col, err := s.svc.GetCollection(collectionName) // fullName
	if err == service.ErrorCollectionNotFound {
		return s.emptyCursor(dbName, collectionName), nil
	}
	if err != nil {
		return nil, err
	}
	filterValue, _ := doc.Get("filter")
	filterMap := map[string]interface{}{}
	if filterDoc, ok := filterValue.(Document); ok {
		filterMap = filterDoc.ToMap()
	}
	limit := int64(0)
	if limitValue, ok := doc.Get("limit"); ok {
		switch v := limitValue.(type) {
		case int32:
			limit = int64(v)
		case int64:
			limit = v
		}
	}
	if limit <= 0 {
		limit = 101
	}
	skip := int64(0)
	if skipValue, ok := doc.Get("skip"); ok {
		switch v := skipValue.(type) {
		case int32:
			skip = int64(v)
		case int64:
			skip = v
		}
	}
	batch := make([]interface{}, 0)
	for _, row := range col.Rows {
		if limit > 0 && int64(len(batch)) >= limit {
			break
		}
		payload := map[string]interface{}{}
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			return nil, err
		}
		match := true
		if len(filterMap) > 0 {
			var err error
			match, err = connor.Match(filterMap, payload)
			if err != nil {
				return nil, err
			}
		}
		if !match {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		batch = append(batch, ensureDocument(payload))
	}
	cursor := NewDocument(
		"firstBatch", NewArray(batch...),
		"id", int64(0),
		"ns", fmt.Sprintf("%s.%s", dbName, collectionName),
	)
	return NewDocument(
		"cursor", cursor,
		"ok", float64(1),
	), nil
}

func (s *Server) emptyCursor(dbName, collection string) *Document {
	cursor := NewDocument(
		"firstBatch", NewArray(),
		"id", int64(0),
		"ns", fmt.Sprintf("%s.%s", dbName, collection),
	)
	return NewDocument(
		"cursor", cursor,
		"ok", float64(1),
	)
}

func (s *Server) extractDatabase(doc Document) (string, bool) {
	if value, ok := doc.Get("$db"); ok {
		if db, ok := value.(string); ok {
			return db, true
		}
	}
	return "default", false
}

func splitNamespace(name string) (string, string) {
	if idx := strings.Index(name, "."); idx >= 0 {
		return name[:idx], name[idx+1:]
	}
	return "default", name
}

func buildNamespace(db, collection string) string {
	if db == "" {
		db = "default"
	}
	if collection == "" {
		return db
	}
	return fmt.Sprintf("%s.%s", db, collection)
}

func ensureDocument(value interface{}) *Document {
	switch v := value.(type) {
	case *Document:
		return v
	case Document:
		return &v
	case map[string]interface{}:
		if doc, ok := normalizeValue(v).(*Document); ok {
			return doc
		}
		return NewDocument()
	case nil:
		return NewDocument()
	default:
		if doc, ok := normalizeValue(v).(*Document); ok {
			return doc
		}
		return NewDocument()
	}
}
