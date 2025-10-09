package collection

import (
	"path/filepath"
	"testing"
)

func BenchmarkPatch(b *testing.B) {
	filename := filepath.Join(b.TempDir(), "collection.db")

	col, err := OpenCollection(filename)
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	defer col.Close()

	row, err := col.Insert(benchmarkSampleDocument())
	if err != nil {
		b.Fatalf("insert row: %v", err)
	}

	patch := benchmarkSamplePatch()
	originalPayload := append([]byte(nil), row.Payload...)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row.Payload = originalPayload
		if err := col.patchByRow(row, patch, false); err != nil {
			b.Fatalf("patch row: %v", err)
		}
	}
}

func benchmarkSampleDocument() map[string]interface{} {
	return map[string]interface{}{
		"id": "user-123",
		"profile": map[string]interface{}{
			"name": "Alice",
			"age":  29,
			"contact": map[string]interface{}{
				"email":  "alice@example.com",
				"phones": []interface{}{"555-0101", "555-0102"},
			},
			"tags": []interface{}{"golang", "databases", "json"},
		},
		"settings": map[string]interface{}{
			"theme":         "light",
			"notifications": map[string]interface{}{"email": true, "sms": true},
			"betaFeature":   true,
		},
		"tags": []interface{}{"alpha", "beta", "gamma"},
	}
}

func benchmarkSamplePatch() map[string]interface{} {
	return map[string]interface{}{
		"profile": map[string]interface{}{
			"age": 30,
			"contact": map[string]interface{}{
				"email":  "alice+updated@example.com",
				"phones": []interface{}{"555-0101"},
			},
		},
		"settings": map[string]interface{}{
			"theme": "dark",
			"notifications": map[string]interface{}{
				"sms":  false,
				"push": true,
			},
			"betaFeature": nil,
		},
		"tags":     []interface{}{"alpha", "delta"},
		"metadata": map[string]interface{}{"lastLogin": "2024-01-01T12:34:56Z", "status": "active"},
	}
}
