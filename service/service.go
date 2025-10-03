package service

import (
	"errors"
	"fmt"
	"io"

	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/database"
)

type Service struct {
	db *database.Database
}

func NewService(db *database.Database) *Service {
	return &Service{db: db}
}

var ErrorCollectionAlreadyExists = errors.New("collection already exists")

func (s *Service) CreateCollection(name string) (*collection.Collection, error) {
	collection, err := s.db.CreateCollection(name)
	if err != nil {
		if errors.Is(err, database.ErrCollectionExists) {
			return nil, ErrorCollectionAlreadyExists
		}
		return nil, err
	}

	return collection, nil
}

func (s *Service) GetCollection(name string) (*collection.Collection, error) {
	collection, exist := s.db.GetCollection(name)
	if !exist {
		return nil, ErrorCollectionNotFound
	}

	return collection, nil
}

func (s *Service) ListCollections() map[string]*collection.Collection {
	return s.db.ListCollections()
}

func (s *Service) DeleteCollection(name string) error {
	if err := s.db.DropCollection(name); err != nil {
		if errors.Is(err, database.ErrCollectionNotFound) {
			return ErrorCollectionNotFound
		}
		return err
	}
	return nil
}

var ErrorInsertBadJson = errors.New("insert bad json")
var ErrorInsertConflict = errors.New("insert conflict")

func (s *Service) Insert(name string, data io.Reader) error {

	collection, exists := s.db.GetCollection(name)
	if !exists {
		// TODO: here create collection :D
		return ErrorCollectionNotFound
	}

	jsonReader := jsontext.NewDecoder(data)

	for {
		item := map[string]interface{}{}
		err := jsonv2.UnmarshalDecode(jsonReader, &item)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			// TODO: handle error properly
			fmt.Println("ERROR:", err.Error())
			return ErrorInsertBadJson
		}
		_, err = collection.Insert(item)
		if err != nil {
			// TODO: handle error properly
			return ErrorInsertConflict
		}

		// jsonWriter.Encode(item)
	}

	return nil
}
