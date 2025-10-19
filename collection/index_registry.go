package collection

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type IndexDefinition struct {
	Type       string
	NewOptions func() interface{}
	Builder    func(options interface{}) (Index, error)

	optionsType reflect.Type
}

var (
	indexRegistryMu         sync.RWMutex
	indexRegistryByType     = map[string]*IndexDefinition{}
	indexRegistryByOptsType = map[reflect.Type]*IndexDefinition{}
)

func RegisterIndex(definition *IndexDefinition) error {
	if definition == nil {
		return fmt.Errorf("definition cannot be nil")
	}
	if definition.Type == "" {
		return fmt.Errorf("definition type cannot be empty")
	}
	if definition.NewOptions == nil {
		return fmt.Errorf("definition NewOptions cannot be nil")
	}
	if definition.Builder == nil {
		return fmt.Errorf("definition Builder cannot be nil")
	}
	options := definition.NewOptions()
	if options == nil {
		return fmt.Errorf("NewOptions for '%s' returned nil", definition.Type)
	}
	optionsType := reflect.TypeOf(options)
	if optionsType.Kind() != reflect.Ptr {
		return fmt.Errorf("NewOptions for '%s' must return a pointer", definition.Type)
	}

	indexRegistryMu.Lock()
	defer indexRegistryMu.Unlock()

	if _, exists := indexRegistryByType[definition.Type]; exists {
		return fmt.Errorf("index type '%s' already registered", definition.Type)
	}
	if _, exists := indexRegistryByOptsType[optionsType]; exists {
		return fmt.Errorf("index options type '%s' already registered", optionsType)
	}

	definition.optionsType = optionsType
	indexRegistryByType[definition.Type] = definition
	indexRegistryByOptsType[optionsType] = definition

	return nil
}

func MustRegisterIndex(definition *IndexDefinition) {
	if err := RegisterIndex(definition); err != nil {
		panic(err)
	}
}

func GetIndexDefinitionByType(typeName string) (*IndexDefinition, error) {
	indexRegistryMu.RLock()
	defer indexRegistryMu.RUnlock()

	definition, exists := indexRegistryByType[typeName]
	if !exists {
		return nil, fmt.Errorf("index type '%s' is not registered", typeName)
	}
	return definition, nil
}

func GetIndexDefinitionByOptions(options interface{}) (*IndexDefinition, error) {
	if options == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}
	optionsType := reflect.TypeOf(options)

	indexRegistryMu.RLock()
	defer indexRegistryMu.RUnlock()

	definition, exists := indexRegistryByOptsType[optionsType]
	if !exists {
		return nil, fmt.Errorf("no index registered for options type '%s'", optionsType)
	}
	return definition, nil
}

func RegisteredIndexTypes() []string {
	indexRegistryMu.RLock()
	defer indexRegistryMu.RUnlock()

	types := make([]string, 0, len(indexRegistryByType))
	for typeName := range indexRegistryByType {
		types = append(types, typeName)
	}
	sort.Strings(types)
	return types
}
