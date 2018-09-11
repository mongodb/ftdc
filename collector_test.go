package ftdc

import (
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
)

func randStr() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randFlatDocument(numKeys int) *bson.Document {
	doc := bson.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(bson.EC.Int64(randStr(), rand.Int63n(int64(numKeys)*1)))
	}

	return doc
}

func randComplexDocument(numKeys, otherNum int) *bson.Document {
	doc := bson.NewDocument()

	for i := 0; i < numKeys; i++ {
		doc.Append(bson.EC.Int64(randStr(), rand.Int63n(int64(numKeys)*1)))

		if otherNum%5 == 0 {
			ar := bson.NewArray()
			for ii := int64(0); i < otherNum; i++ {
				ar.Append(bson.VC.Int64(rand.Int63n(1 + ii*int64(numKeys))))
			}
			doc.Append(bson.EC.Array(randStr(), ar))
		}

		if otherNum%3 == 0 {
			doc.Append(bson.EC.SubDocument(randStr(), randFlatDocument(otherNum)))
		}

		if otherNum%12 == 0 {
			doc.Append(bson.EC.SubDocument(randStr(), randComplexDocument(otherNum, 10)))
		}

	}

	return doc
}

func TestCollectorInterface(t *testing.T) {
	if testing.Short() {
		t.Skip("a large test table")
	}

	t.Parallel()
	for _, collect := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Basic",
			factory: func() Collector { return NewBasicCollector() },
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
			factory: func() Collector { return NewSamplingCollector(0, NewBasicCollector()) },
		},
	} {
		t.Run(collect.name, func(t *testing.T) {
			for _, test := range []struct {
				name      string
				docs      []*bson.Document
				numStats  int
				randStats bool
			}{
				{
					name: "NoDocuments",
				},
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
				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()

					assert.NotPanics(t, func() {
						collector.SetMetadata(createEventRecord(42, int64(time.Minute), rand.Int63n(7), 4))
					})

					info := collector.Info()
					assert.Zero(t, info)

					for _, d := range test.docs {
						assert.NoError(t, collector.Add(d))
					}
					info = collector.Info()

					assert.Equal(t, len(test.docs), info.SampleCount)
					if test.randStats {
						assert.True(t, info.MetricsCount >= test.numStats,
							"%d >= %d", info.MetricsCount, test.numStats)
					} else {
						assert.Equal(t, test.numStats, info.MetricsCount)
					}

					out, err := collector.Resolve()
					if len(test.docs) > 0 {
						assert.NoError(t, err)
						assert.NotZero(t, out)
					} else {
						assert.Error(t, err)
						assert.Zero(t, out)
					}

					collector.Reset()
					info = collector.Info()
					assert.Zero(t, info)
				})
			}
		})
	}
}
