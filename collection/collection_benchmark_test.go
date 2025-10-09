package collection

import (
	"encoding/json"
	"testing"
)

func BenchmarkPatch(b *testing.B) {
	b.Run("map", func(b *testing.B) {
		runPatchBenchmark(b, []interface{}{
			map[string]any{"name": "Jaime"},
			map[string]any{"name": "Pablo"},
		})
	})

	b.Run("raw", func(b *testing.B) {
		runPatchBenchmark(b, []interface{}{
			json.RawMessage(`{"name":"Jaime"}`),
			json.RawMessage(`{"name":"Pablo"}`),
		})
	})
}

func runPatchBenchmark(b *testing.B, patches []interface{}) {
	Environment(func(filename string) {
		c, err := OpenCollection(filename)
		if err != nil {
			b.Fatalf("open collection: %v", err)
		}
		defer c.Close()

		row, err := c.Insert(map[string]any{"id": "1", "name": "Pablo"})
		if err != nil {
			b.Fatalf("insert: %v", err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := c.Patch(row, patches[i%len(patches)]); err != nil {
				b.Fatalf("patch: %v", err)
			}
		}
	})
}
