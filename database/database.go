package database

import (
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

type Config struct {
	Dir string
}

type Database struct {
	Config      *Config
	status      string
	Collections map[string]*collection.Collection
	exit        chan struct{}
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

	_, exists := db.Collections[name]
	if exists {
		return nil, fmt.Errorf("collection '%s' already exists", name)
	}

	filename := path.Join(db.Config.Dir, name)
	col, err := collection.OpenCollection(filename)
	if err != nil {
		return nil, err
	}

	db.Collections[name] = col

	return col, nil
}

func (db *Database) DropCollection(name string) error { // TODO: rename drop?

	col, exists := db.Collections[name]
	if !exists {
		return fmt.Errorf("collection '%s' not found", name)
	}

	filename := path.Join(db.Config.Dir, name)

	err := os.Remove(filename)
	if err != nil {
		return err // TODO: wrap?
	}

	delete(db.Collections, name) // TODO: protect section! not threadsafe

	return col.Close()
}

func (db *Database) Load() error {

	fmt.Printf("Loading database %s...\n", db.Config.Dir) // todo: move to logger
	dir := db.Config.Dir
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	type collectionEntry struct {
		name     string
		filename string
	}

	entries := []collectionEntry{}

	err = filepath.WalkDir(dir, func(filename string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		name := filename
		name = strings.TrimPrefix(name, dir)
		name = strings.TrimPrefix(name, "/")

		entries = append(entries, collectionEntry{name: name, filename: filename})

		return nil
	})

	if err != nil {
		db.status = StatusClosing
		return err
	}

	collections := make(map[string]*collection.Collection, len(entries))
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	workerLimit := runtime.NumCPU()
	if workerLimit < 1 {
		workerLimit = 1
	}
	sem := make(chan struct{}, workerLimit)

	for _, entry := range entries {
		entry := entry
		wg.Add(1)
		go func() {
			sem <- struct{}{}
			defer func() {
				<-sem
				wg.Done()
			}()

			t0 := time.Now()
			col, err := collection.OpenCollection(entry.filename)
			if err != nil {
				fmt.Printf("ERROR: open collection '%s': %s\n", entry.filename, err.Error()) // todo: move to logger
				errOnce.Do(func() {
					firstErr = err
				})
				return
			}
			fmt.Println(entry.name, len(col.Rows), time.Since(t0)) // todo: move to logger

			mu.Lock()
			collections[entry.name] = col
			mu.Unlock()
		}()
	}

	wg.Wait()

	if firstErr != nil {
		for _, col := range collections {
			col.Close()
		}
		db.status = StatusClosing
		return firstErr
	}

	for name := range db.Collections {
		delete(db.Collections, name)
	}
	for name, col := range collections {
		db.Collections[name] = col
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
	for name, col := range db.Collections {
		fmt.Printf("Closing '%s'...\n", name)
		err := col.Close()
		if err != nil {
			fmt.Printf("ERROR: close(%s): %s", name, err.Error())
			lastErr = err
		}
	}

	return lastErr
}
