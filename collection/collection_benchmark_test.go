package collection

import (
	"path/filepath"
	"testing"
)

func BenchmarkCollectionInsert(b *testing.B) {
	dir := b.TempDir()
	filename := filepath.Join(dir, "benchmark.db")

	collection, err := OpenCollection(filename)
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	defer collection.Close()

	item := map[string]any{"value": 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item["value"] = i
		if _, err := collection.Insert(item); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}
