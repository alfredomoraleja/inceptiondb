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

	type loadTask struct {
		filename string
		name     string
	}

	tasks := []loadTask{}
	err = filepath.WalkDir(dir, func(filename string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		name := strings.TrimPrefix(filename, dir)
		name = strings.TrimPrefix(name, "/")

		tasks = append(tasks, loadTask{filename: filename, name: name})
		return nil
	})
	if err != nil {
		db.status = StatusClosing
		return err
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount <= 0 {
		workerCount = 1
	}
	if len(tasks) < workerCount {
		workerCount = len(tasks)
	}

	type loadResult struct {
		task       loadTask
		collection *collection.Collection
		err        error
		rows       int
		duration   time.Duration
	}

	jobs := make(chan loadTask)
	results := make(chan loadResult)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				t0 := time.Now()
				col, err := collection.OpenCollection(task.filename)
				if err != nil {
					results <- loadResult{task: task, err: err}
					continue
				}
				results <- loadResult{task: task, collection: col, rows: len(col.Rows), duration: time.Since(t0)}
			}
		}()
	}

	go func() {
		for _, task := range tasks {
			jobs <- task
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for res := range results {
		if res.err != nil {
			fmt.Printf("ERROR: open collection '%s': %s\n", res.task.filename, res.err.Error()) // todo: move to logger
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}

		fmt.Println(res.task.name, res.rows, res.duration) // todo: move to logger
		db.Collections[res.task.name] = res.collection
	}

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
