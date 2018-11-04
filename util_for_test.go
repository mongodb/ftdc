package ftdc

import (
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/mongodb/mongo-go-driver/bson"
)

type customCollector struct {
	name    string
	factory func() Collector
}

type customTest struct {
	name      string
	docs      []*bson.Document
	numStats  int
	randStats bool
}

func createEventRecord(count, duration, size, workers int64) *bson.Document {
	return bson.NewDocument(
		bson.EC.Int64("count", count),
		bson.EC.Int64("duration", duration),
		bson.EC.Int64("size", size),
		bson.EC.Int64("workers", workers),
	)
}

func randStr() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func randFlatDocument(numKeys int) *bson.Document {
	doc := bson.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(bson.EC.Int64(fmt.Sprint(i), rand.Int63n(int64(numKeys)*1)))
	}

	return doc
}

func randFlatDocumentWithFloats(numKeys int) *bson.Document {
	doc := bson.NewDocument()
	for i := 0; i < numKeys; i++ {
		doc.Append(bson.EC.Double(fmt.Sprint(i), rand.Float64()))
	}
	return doc
}

func randComplexDocument(numKeys, otherNum int) *bson.Document {
	doc := bson.NewDocument()

	for i := 0; i < numKeys; i++ {
		doc.Append(bson.EC.Int64(fmt.Sprintln(numKeys, otherNum), rand.Int63n(int64(numKeys)*1)))

		if otherNum%5 == 0 {
			ar := bson.NewArray()
			for ii := int64(0); i < otherNum; i++ {
				ar.Append(bson.VC.Int64(rand.Int63n(1 + ii*int64(numKeys))))
			}
			doc.Append(bson.EC.Array(fmt.Sprintln("first", numKeys, otherNum), ar))
		}

		if otherNum%3 == 0 {
			doc.Append(bson.EC.SubDocument(fmt.Sprintln("second", numKeys, otherNum), randFlatDocument(otherNum)))
		}

		if otherNum%12 == 0 {
			doc.Append(bson.EC.SubDocument(fmt.Sprintln("third", numKeys, otherNum), randComplexDocument(otherNum, 10)))
		}
	}

	return doc
}

func createCollectors() []*customCollector {
	collectors := []*customCollector{
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
	}
	return collectors
}

func createTests() []*customTest {
	tests := []*customTest{
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
		{
			name: "Floats",
			docs: []*bson.Document{
				randFlatDocumentWithFloats(1),
			},
			randStats: true,
			numStats:  1,
		},
	}
	return tests
}
