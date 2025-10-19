package apireplicationv1

import (
	"context"

	"github.com/fulldump/inceptiondb/database"
	"github.com/fulldump/inceptiondb/replication"
)

type contextKey string

const (
	contextDatabaseKey contextKey = "c46be5dc-02b0-4ecf-a38f-5c721a4d9d83"
	contextManagerKey  contextKey = "51acb646-8338-4f4a-a19d-5b2b050f0b1c"
)

func SetDatabase(ctx context.Context, db *database.Database) context.Context {
	return context.WithValue(ctx, contextDatabaseKey, db)
}

func GetDatabase(ctx context.Context) *database.Database {
	if ctx == nil {
		return nil
	}
	db, _ := ctx.Value(contextDatabaseKey).(*database.Database)
	return db
}

func SetManager(ctx context.Context, manager *replication.Manager) context.Context {
	return context.WithValue(ctx, contextManagerKey, manager)
}

func GetManager(ctx context.Context) *replication.Manager {
	if ctx == nil {
		return nil
	}
	manager, _ := ctx.Value(contextManagerKey).(*replication.Manager)
	return manager
}
