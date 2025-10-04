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

	type loadJob struct {
		filename string
		name     string
	}

	jobs := make([]loadJob, 0)
	err = filepath.WalkDir(dir, func(filename string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		name := strings.TrimPrefix(filename, dir)
		name = strings.TrimPrefix(name, "/")

		jobs = append(jobs, loadJob{filename: filename, name: name})
		return nil
	})

	if err != nil {
		db.status = StatusClosing
		return err
	}

	type loadResult struct {
		job      loadJob
		col      *collection.Collection
		duration time.Duration
		err      error
	}

	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}

	jobsCh := make(chan loadJob)
	resultsCh := make(chan loadResult, len(jobs))
	var workers sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobsCh {
				start := time.Now()
				col, err := collection.OpenCollection(job.filename)
				resultsCh <- loadResult{
					job:      job,
					col:      col,
					duration: time.Since(start),
					err:      err,
				}
			}
		}()
	}

	go func() {
		workers.Wait()
		close(resultsCh)
	}()

	for _, job := range jobs {
		jobsCh <- job
	}
	close(jobsCh)

	var firstErr error
	for result := range resultsCh {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			fmt.Printf("ERROR: open collection '%s': %s\n", result.job.filename, result.err.Error())
			continue
		}

		fmt.Println(result.job.name, len(result.col.Rows), result.duration) // todo: move to logger

		db.Collections[result.job.name] = result.col
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
