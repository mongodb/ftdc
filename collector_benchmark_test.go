package ftdc

import (
	"math/rand"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
)

var result []byte

func BenchmarkCollectorInterface(b *testing.B) {
	for _, collect := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{} },
		},
		{
			name:    "SmallBatch",
			factory: func() Collector { return NewBatchCollector(10) },
		},
		{
			name:    "MediumBatch",
			factory: func() Collector { return NewBatchCollector(100) },
		},
		{
			name:    "LargeBatch",
			factory: func() Collector { return NewBatchCollector(1000) },
		},
		{
			name:    "XtraLargeBatch",
			factory: func() Collector { return NewBatchCollector(10000) },
		},
		{
			name:    "SuperXtraLargeBatch",
			factory: func() Collector { return NewBatchCollector(100000) },
		},
		{
			name:    "SmallDynamic",
			factory: func() Collector { return NewDynamicCollector(10) },
		},
		{
			name:    "MediumDynamic",
			factory: func() Collector { return NewDynamicCollector(100) },
		},
		{
			name:    "LargeDynamic",
			factory: func() Collector { return NewDynamicCollector(1000) },
		},
		{
			name:    "XtraLargeDynamic",
			factory: func() Collector { return NewDynamicCollector(10000) },
		},
		{
			name:    "SuperXtraLargeDynamic",
			factory: func() Collector { return NewDynamicCollector(100000) },
		},
		{
			name:    "SampleBasic",
			factory: func() Collector { return NewSamplingCollector(0, &betterCollector{}) },
		},
	} {
		b.Run(collect.name, func(b *testing.B) {
			for _, test := range []struct {
				name      string
				docs      []*bson.Document
				numStats  int
				randStats bool
			}{
				{
					name: "OneDocNoStats",
					docs: []*bson.Document{
						bson.NewDocument(bson.EC.String("foo", "bar")),
					},
				},
				{
					name: "OneDocumentOneStat",
					docs: []*bson.Document{
						bson.NewDocument(bson.EC.Int32("foo", 42)),
					},
					numStats: 1,
				},
				{
					name: "OneSmallFlat",
					docs: []*bson.Document{
						randFlatDocument(12),
					},
					numStats: 12,
				},
				{
					name: "OneLargeFlat",
					docs: []*bson.Document{
						randFlatDocument(360),
					},
					numStats: 360,
				},
				{
					name: "OneHugeFlat",
					docs: []*bson.Document{
						randFlatDocument(36000),
					},
					numStats: 36000,
				},
				{
					name: "SeveralDocNoStats",
					docs: []*bson.Document{
						bson.NewDocument(bson.EC.String("foo", "bar")),
						bson.NewDocument(bson.EC.String("foo", "bar")),
						bson.NewDocument(bson.EC.String("foo", "bar")),
						bson.NewDocument(bson.EC.String("foo", "bar")),
					},
				},
				{
					name: "SeveralDocumentOneStat",
					docs: []*bson.Document{
						bson.NewDocument(bson.EC.Int32("foo", 42)),
						bson.NewDocument(bson.EC.Int32("foo", 42)),
						bson.NewDocument(bson.EC.Int32("foo", 42)),
						bson.NewDocument(bson.EC.Int32("foo", 42)),
						bson.NewDocument(bson.EC.Int32("foo", 42)),
					},
					numStats: 1,
				},
				{
					name: "SeveralSmallFlat",
					docs: []*bson.Document{
						randFlatDocument(12),
						randFlatDocument(12),
						randFlatDocument(12),
						randFlatDocument(12),
					},
					randStats: true,
					numStats:  12,
				},
				{
					name: "SeveralLargeFlat",
					docs: []*bson.Document{
						randFlatDocument(200),
						randFlatDocument(200),
						randFlatDocument(200),
						randFlatDocument(200),
					},
					randStats: true,
					numStats:  200,
				},
				{
					name: "SeveralHugeFlat",
					docs: []*bson.Document{
						randFlatDocument(2000),
						randFlatDocument(2000),
						randFlatDocument(2000),
						randFlatDocument(2000),
					},
					randStats: true,
					numStats:  2000,
				},
				{
					name: "OneSmallRandomComplexDocument",
					docs: []*bson.Document{
						randComplexDocument(4, 10),
					},
					randStats: true,
					numStats:  11,
				},
				{
					name: "OneLargeRandomComplexDocument",
					docs: []*bson.Document{
						randComplexDocument(100, 100),
					},
					randStats: true,
					numStats:  101,
				},
				{
					name: "SeveralSmallRandomComplexDocument",
					docs: []*bson.Document{
						randComplexDocument(4, 100),
						randComplexDocument(4, 100),
						randComplexDocument(4, 100),
					},
					numStats:  101,
					randStats: true,
				},
				{
					name: "OneHugeRandomComplexDocument",
					docs: []*bson.Document{
						randComplexDocument(10000, 10000),
					},
					randStats: true,
					numStats:  1000,
				},
				{
					name: "SeveralHugeRandomComplexDocument",
					docs: []*bson.Document{
						randComplexDocument(10000, 10000),
						randComplexDocument(10000, 10000),
						randComplexDocument(10000, 10000),
						randComplexDocument(10000, 10000),
						randComplexDocument(10000, 10000),
					},
					randStats: true,
					numStats:  1000,
				},
			} {
				b.Run(test.name, func(b *testing.B) {
					collector := collect.factory()
					b.Run("SetMetdaData", func(b *testing.B) {
						doc := createEventRecord(42, int64(time.Minute), rand.Int63n(7), 4)
						for n := 0; n < b.N; n++ {
							collector.SetMetadata(doc)
						}
					})
					b.Run("Add", func(b *testing.B) {
						for n := 0; n < b.N; n++ {
							collector.Add(test.docs[n%len(test.docs)])
						}
					})
					b.Run("Resolve", func(b *testing.B) {
						for n := 0; n < b.N; n++ {
							r, _ := collector.Resolve()
							result = r
						}
					})
				})
			}
		})
	}
}
