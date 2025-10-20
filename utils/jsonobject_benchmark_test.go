package utils

import (
	"encoding/json"
	"fmt"
	"testing"
)

var (
	benchmarkMapData        map[string]any
	benchmarkJSONObjectData JSONObject
	benchmarkJSONPayload    []byte
)

func init() {
	benchmarkMapData, benchmarkJSONObjectData = buildBenchmarkDocuments(10)
	benchmarkJSONPayload = mustMarshal(benchmarkMapData)

	// e := json.NewEncoder(os.Stdout)
	// e.SetIndent("", "  ")
	// e.Encode(benchmarkJSONObjectData)
}

func BenchmarkMapMarshalJSON(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(benchmarkMapData); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONObjectMarshalJSON(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(benchmarkJSONObjectData); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMapUnmarshalJSON(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkJSONPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out map[string]any
		if err := json.Unmarshal(benchmarkJSONPayload, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONObjectUnmarshalJSON(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkJSONPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var obj JSONObject
		if err := json.Unmarshal(benchmarkJSONPayload, &obj); err != nil {
			b.Fatal(err)
		}
	}
}

func buildBenchmarkDocuments(size int) (map[string]any, JSONObject) {
	m := make(map[string]any, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("field_%03d", i)
		m[key] = map[string]any{
			"name":    fmt.Sprintf("name-%d", i),
			"count":   i,
			"enabled": i%2 == 0,
			"tags": []any{
				fmt.Sprintf("tag-%d", i),
				fmt.Sprintf("tag-%d", i+1),
				i,
			},
			"metrics": map[string]any{
				"score":   float64(i) * 1.5,
				"ratio":   float64(i%7) / 7.0,
				"weights": []any{float64(i) / 2.0, float64(i) / 3.0},
			},
		}
	}
	return m, NewJSONObjectFromMap(m)
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
