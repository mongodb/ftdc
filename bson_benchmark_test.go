package ftdc

import (
	"bytes"
	"context"
	"testing"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type metricHashFunc func(*bsonx.Document) (string, int)

func BenchmarkHashBSON(b *testing.B) {
	for _, impl := range []struct {
		Name     string
		HashFunc metricHashFunc
	}{
		{
			Name:     "Legacy",
			HashFunc: metricsHash,
		},
		{
			Name:     "FNVChecksum",
			HashFunc: metricKeyHash,
		},
		{
			Name:     "SHA1Checksum",
			HashFunc: metricKeySHA1,
		},
		{
			Name:     "MD5Checksum",
			HashFunc: metricKeyMD5,
		},
	} {
		b.Run(impl.Name, func(b *testing.B) {
			for _, test := range []struct {
				Name string
				Doc  *bsonx.Document
			}{
				{
					Name: "FlatSmall",
					Doc:  randFlatDocument(10),
				},
				{
					Name: "FlatLarge",
					Doc:  randFlatDocument(100),
				},
				{
					Name: "ComplexSmall",
					Doc:  randComplexDocument(10, 5),
				},
				{
					Name: "ComplexLarge",
					Doc:  randComplexDocument(100, 5),
				},
				{
					Name: "MoreComplexSmall",
					Doc:  randComplexDocument(10, 2),
				},
				{
					Name: "MoreComplexLarge",
					Doc:  randComplexDocument(100, 2),
				},
				{
					Name: "EventMock",
					Doc:  createEventRecord(2, 2, 2, 2),
				},
			} {
				b.Run(test.Name, func(b *testing.B) {
					var (
						h   string
						num int
					)
					for n := 0; n < b.N; n++ {
						h, num = impl.HashFunc(test.Doc)
					}
					b.StopTimer()
					assert.NotZero(b, num)
					assert.NotZero(b, h)
				})
			}
		})
	}
}

func produceMockMetrics(ctx context.Context, samples int, newDoc func() *bsonx.Document) []Metric {
	collector := NewBaseCollector(samples)
	for i := 0; i < samples; i++ {
		if err := collector.Add(newDoc()); err != nil {
			panic(err)
		}
	}
	payload, err := collector.Resolve()
	if err != nil {
		panic(err)
	}

	iter := ReadChunks(ctx, bytes.NewBuffer(payload))
	if !iter.Next() {
		panic("could not iterate")
	}

	metrics := iter.Chunk().metrics
	iter.Close()
	return metrics
}

func BenchmarkDocumentCreation(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, test := range []struct {
		Name      string
		Samples   int
		Length    int
		Reference *bsonx.Document
		Metrics   []Metric
	}{
		{
			Name:      "Flat",
			Samples:   1000,
			Length:    15,
			Reference: randFlatDocument(15),
			Metrics:   produceMockMetrics(ctx, 1000, func() *bsonx.Document { return randFlatDocument(15) }),
		},
		{
			Name:      "SmallFlat",
			Samples:   1000,
			Length:    5,
			Reference: randFlatDocument(5),
			Metrics:   produceMockMetrics(ctx, 1000, func() *bsonx.Document { return randFlatDocument(5) }),
		},
		{
			Name:      "LargeFlat",
			Samples:   1000,
			Length:    15,
			Reference: randFlatDocument(15),
			Metrics:   produceMockMetrics(ctx, 1000, func() *bsonx.Document { return randFlatDocument(100) }),
		},
		{
			Name:      "Complex",
			Samples:   1000,
			Length:    40,
			Reference: randComplexDocument(20, 3),
			Metrics:   produceMockMetrics(ctx, 1000, func() *bsonx.Document { return randComplexDocument(20, 3) }),
		},
		{
			Name:      "SmallComplex",
			Samples:   1000,
			Length:    5,
			Reference: randComplexDocument(5, 1),
			Metrics:   produceMockMetrics(ctx, 1000, func() *bsonx.Document { return randComplexDocument(5, 1) }),
		},
	} {
		var doc *bsonx.Document
		b.Run(test.Name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				for i := 0; i < test.Samples; i++ {
					doc, _ = rehydrateDocument(test.Reference, i, test.Metrics, 0)
					require.NotNil(b, doc)
					require.Equal(b, test.Length, doc.Len())
				}
			}
		})
	}
}
