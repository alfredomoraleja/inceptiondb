package database

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fulldump/inceptiondb/collection"
)

const (
	StatusOpening   = "opening"
	StatusOperating = "operating"
	StatusClosing   = "closing"
)

var (
	ErrCollectionExists   = errors.New("collection already exists")
	ErrCollectionNotFound = errors.New("collection not found")
)

type Config struct {
	Dir string
}

type Database struct {
	Config        *Config
	status        string
	Collections   map[string]*collection.Collection
	collectionsMu sync.RWMutex
	exit          chan struct{}
}

func NewDatabase(config *Config) *Database { // todo: return error?
	s := &Database{
		Config:      config,
		status:      StatusOpening,
		Collections: map[string]*collection.Collection{},
		exit:        make(chan struct{}),
	}

	return s
}

func (db *Database) GetStatus() string {
	return db.status
}

func (db *Database) CreateCollection(name string) (*collection.Collection, error) {

	db.collectionsMu.RLock()
	_, exists := db.Collections[name]
	db.collectionsMu.RUnlock()
	if exists {
		return nil, ErrCollectionExists
	}

	filename := path.Join(db.Config.Dir, name)
	col, err := collection.OpenCollection(filename)
	if err != nil {
		return nil, err
	}

	db.collectionsMu.Lock()
	if _, exists := db.Collections[name]; exists {
		db.collectionsMu.Unlock()
		col.Close()
		return nil, ErrCollectionExists
	}
	db.Collections[name] = col
	db.collectionsMu.Unlock()

	return col, nil
}

func (db *Database) GetCollection(name string) (*collection.Collection, bool) {
	db.collectionsMu.RLock()
	defer db.collectionsMu.RUnlock()

	col, ok := db.Collections[name]
	return col, ok
}

func (db *Database) ListCollections() map[string]*collection.Collection {
	db.collectionsMu.RLock()
	defer db.collectionsMu.RUnlock()

	result := make(map[string]*collection.Collection, len(db.Collections))
	for name, col := range db.Collections {
		result[name] = col
	}

	return result
}

func (db *Database) DropCollection(name string) error { // TODO: rename drop?

	db.collectionsMu.Lock()
	col, exists := db.Collections[name]
	if !exists {
		db.collectionsMu.Unlock()
		return ErrCollectionNotFound
	}
	delete(db.Collections, name)
	db.collectionsMu.Unlock()

	if err := col.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	filename := path.Join(db.Config.Dir, name)

	if err := os.Remove(filename); err != nil {
		return err // TODO: wrap?
	}

	return nil
}

func (db *Database) Load() error {

	fmt.Printf("Loading database %s...\n", db.Config.Dir) // todo: move to logger
	dir := db.Config.Dir
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	filenames := make([]string, 0)
	err = filepath.WalkDir(dir, func(filename string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		filenames = append(filenames, filename)
		return nil
	})

	if err != nil {
		db.status = StatusClosing
		return err
	}

	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMu sync.Mutex

	for _, filename := range filenames {
		filename := filename
		wg.Add(1)
		go func() {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			name := strings.TrimPrefix(filename, dir)
			name = strings.TrimPrefix(name, string(os.PathSeparator))

			t0 := time.Now()
			col, err := collection.OpenCollection(filename)
			if err != nil {
				fmt.Printf("ERROR: open collection '%s': %s\n", filename, err.Error()) // todo: move to logger
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
				return
			}
			fmt.Println(name, len(col.Rows), time.Since(t0)) // todo: move to logger

			db.collectionsMu.Lock()
			db.Collections[name] = col
			db.collectionsMu.Unlock()
		}()
	}

	wg.Wait()

	if firstErr != nil {
		db.status = StatusClosing
		return firstErr
	}

	fmt.Println("Ready")

	db.status = StatusOperating

	return nil

}

func (db *Database) Start() error {

	go db.Load()

	<-db.exit

	return nil
}

func (db *Database) Stop() error {

	defer close(db.exit)

	db.status = StatusClosing

	var lastErr error
	collections := db.ListCollections()
	for name, col := range collections {
		fmt.Printf("Closing '%s'...\n", name)
		err := col.Close()
		if err != nil {
			fmt.Printf("ERROR: close(%s): %s", name, err.Error())
			lastErr = err
		}
	}

	return lastErr
}
