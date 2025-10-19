package database

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/persistence"
)

const (
	StatusOpening   = "opening"
	StatusOperating = "operating"
	StatusClosing   = "closing"
)

type Config struct {
	Dir         string
	Persistence string
}

type Database struct {
	Config      *Config
	status      string
	Collections map[string]*collection.Collection
	exit        chan struct{}
	driver      persistence.Driver
}

func NewDatabase(config *Config) (*Database, error) {
	driverName := config.Persistence
	if driverName == "" {
		driverName = persistence.DefaultDriverName
	}

	driver, err := persistence.GetDriver(driverName)
	if err != nil {
		return nil, err
	}

	s := &Database{
		Config:      config,
		status:      StatusOpening,
		Collections: map[string]*collection.Collection{},
		exit:        make(chan struct{}),
		driver:      driver,
	}

	return s, nil
}

func (db *Database) GetStatus() string {
	return db.status
}

func (db *Database) PersistenceDriver() persistence.Driver {
	return db.driver
}

func (db *Database) CreateCollection(name string) (*collection.Collection, error) {

	_, exists := db.Collections[name]
	if exists {
		return nil, fmt.Errorf("collection '%s' already exists", name)
	}

	filename := path.Join(db.Config.Dir, name)
	col, err := collection.OpenCollection(filename, db.driver)
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

	err := db.driver.Remove(filename)
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

		t0 := time.Now()
		col, err := collection.OpenCollection(filename, db.driver)
		if err != nil {
			fmt.Printf("ERROR: open collection '%s': %s\n", filename, err.Error()) // todo: move to logger
			return err
		}
		fmt.Println(name, len(col.Rows), time.Since(t0)) // todo: move to logger

		db.Collections[name] = col

		return nil
	})

	if err != nil {
		db.status = StatusClosing
		return err
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
