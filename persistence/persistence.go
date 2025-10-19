package persistence

import (
	"fmt"
	"io"
	"sync"
)

type Writer interface {
	io.Writer
	Flush() error
	Close() error
}

type Store interface {
	Reader() (io.ReadCloser, error)
	Writer() (Writer, error)
	Close() error
}

type Driver interface {
	Open(path string) (Store, error)
	Remove(path string) error
}

var (
	driversMu sync.RWMutex
	drivers   = map[string]Driver{}
)

const DefaultDriverName = "file"

func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if name == "" {
		panic("persistence: driver name cannot be empty")
	}
	if driver == nil {
		panic("persistence: driver cannot be nil")
	}
	if _, exists := drivers[name]; exists {
		panic(fmt.Sprintf("persistence: driver '%s' already registered", name))
	}
	drivers[name] = driver
}

func GetDriver(name string) (Driver, error) {
	driversMu.RLock()
	driver, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: driver '%s' not registered", name)
	}
	return driver, nil
}

func MustDriver(name string) Driver {
	driver, err := GetDriver(name)
	if err != nil {
		panic(err)
	}
	return driver
}

func RegisteredDrivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	result := make([]string, 0, len(drivers))
	for name := range drivers {
		result = append(result, name)
	}
	return result
}
