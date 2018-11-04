package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorInterface(t *testing.T) {
	if testing.Short() {
		t.Skip("a large test table")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// t.Parallel()
	collectors := createCollectors()
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()
			for _, test := range tests {
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
						if !assert.Equal(t, test.numStats, info.MetricsCount) {
							fmt.Println(test.docs)
							fmt.Println(info)
						}
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
			t.Run("RoundTrip", func(t *testing.T) {
				for name, docs := range map[string][]*bson.Document{
					"Integers": []*bson.Document{
						randFlatDocument(5),
						randFlatDocument(5),
						// randFlatDocument(5),
						// randFlatDocument(5),
					},
					// "Floats": []*bson.Document{
					// 	randFlatDocumentWithFloats(5),
					// 	randFlatDocumentWithFloats(5),
					// 	randFlatDocumentWithFloats(5),
					// 	randFlatDocumentWithFloats(5),
					// },
					"DecendingHandIntegers": []*bson.Document{
						bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5)),
						bson.NewDocument(bson.EC.Int64("one", 89), bson.EC.Int64("two", 4)),
						// bson.NewDocument(bson.EC.Int64("one", 99), bson.EC.Int64("two", 3)),
						// bson.NewDocument(bson.EC.Int64("one", 101), bson.EC.Int64("two", 2)),
					},
					"HandMixed": []*bson.Document{
						bson.NewDocument(bson.EC.Double("one", 4.33333), bson.EC.Int64("two", 999)),
						bson.NewDocument(bson.EC.Double("one", 3.88), bson.EC.Int64("two", 410)),
						// bson.NewDocument(bson.EC.Double("one", 2.4343), bson.EC.Int64("two", 43)),
						// bson.NewDocument(bson.EC.Double("one", 1.43), bson.EC.Int64("two", 43)),
					},
				} {
					t.Run(name, func(t *testing.T) {
						collector := collect.factory()
						count := 0
						for _, d := range docs {
							count++
							assert.NoError(t, collector.Add(d))
						}
						info := collector.Info()
						require.Equal(t, info.SampleCount, count)

						out, err := collector.Resolve()
						require.NoError(t, err)
						buf := bytes.NewBuffer(out)

						iter := ReadStructuredMetrics(ctx, buf)
						idx := -1
						for iter.Next(ctx) {
							idx++
							t.Run(fmt.Sprintf("DocumentNumber_%d", idx), func(t *testing.T) {
								s := iter.Document()

								if !assert.True(t, s.Equal(docs[idx])) {
									fmt.Println("---", idx)
									fmt.Println("in: ", docs[idx])
									fmt.Println("out:", s)
								}
							})
						}
						assert.Equal(t, len(docs)-1, idx) // zero index
						require.NoError(t, iter.Err())

					})
				}

			})
		})
	}
}
