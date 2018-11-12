package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorInterface(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Parallel()
	collectors := createCollectors()
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()

			for _, test := range tests {
				if testing.Short() {
					continue
				}

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
			t.Run("ResolveWhenNil", func(t *testing.T) {
				collector := collect.factory()
				out, err := collector.Resolve()
				assert.Nil(t, out)
				assert.Error(t, err)
			})
			t.Run("RoundTrip", func(t *testing.T) {
				for name, docs := range map[string][]*bson.Document{
					"Integers": []*bson.Document{
						randFlatDocument(5),
						randFlatDocument(5),
						randFlatDocument(5),
						randFlatDocument(5),
					},
					"DecendingHandIntegers": []*bson.Document{
						bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5)),
						bson.NewDocument(bson.EC.Int64("one", 89), bson.EC.Int64("two", 4)),
						bson.NewDocument(bson.EC.Int64("one", 99), bson.EC.Int64("two", 3)),
						bson.NewDocument(bson.EC.Int64("one", 101), bson.EC.Int64("two", 2)),
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

func TestStreamingEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		name    string
		factory func() (Collector, *bytes.Buffer)
	}{
		{
			name: "StreamingDynamic",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(100, buf), buf
			},
		},
		{
			name: "StreamingDynamicSmall",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(2, buf), buf
			},
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						for _, val := range test.dataset {
							assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("foo", val))))
						}
						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							assert.Equal(t, val, test.dataset[idx])
							idx++
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						if !assert.Equal(t, test.dataset, res) {
							grip.Infoln("in:", test.dataset)
							grip.Infoln("out:", res)
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*bson.Document{}

						for _, val := range test.dataset {
							doc := bson.NewDocument(
								bson.EC.Int64("foo", val),
								bson.EC.Int64("dub", 2*val),
								bson.EC.Int64("dup", val),
								bson.EC.Int64("neg", -1*val),
								bson.EC.Int64("mag", 10*val),
							)
							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if !doc.Equal(docs[idx]) {
								grip.Infoln(idx, "src:", test.dataset[idx])
								grip.Infoln(idx, "in: ", docs[idx].ToExtJSON(false))
								grip.Infoln(idx, "out:", doc.ToExtJSON(false))
							}
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						assert.Equal(t, test.dataset, res)
					})

					t.Run("MultiValueKeyOrder", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*bson.Document{}

						for idx, val := range test.dataset {
							var doc *bson.Document
							if len(test.dataset) >= 3 && (idx == 2 || idx == 3) {
								doc = bson.NewDocument(
									bson.EC.Int64("foo", val),
									bson.EC.Int64("mag", 10*val),
									bson.EC.Int64("neg", -1*val),
								)
							} else {
								doc = bson.NewDocument(
									bson.EC.Int64("foo", val),
									bson.EC.Int64("dub", 2*val),
									bson.EC.Int64("dup", val),
									bson.EC.Int64("neg", -1*val),
									bson.EC.Int64("mag", 10*val),
								)
							}

							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}
						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if !doc.Equal(docs[idx]) {
								grip.Infoln(idx, "src:", test.dataset[idx])
								grip.Infoln(idx, "in: ", docs[idx].ToExtJSON(false))
								grip.Infoln(idx, "out:", doc.ToExtJSON(false))
							}
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res), "%v -> %v", test.dataset, res)
						assert.Equal(t, test.dataset, res)
					})
					t.Run("DifferentKeys", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []*bson.Document{}

						for idx, val := range test.dataset {
							var doc *bson.Document
							if len(test.dataset) >= 5 && (idx == 2 || idx == 3) {
								doc = bson.NewDocument(
									bson.EC.Int64("foo", val),
									bson.EC.Int64("dub", 2*val),
									bson.EC.Int64("dup", val),
									bson.EC.Int64("neg", -1*val),
									bson.EC.Int64("mag", 10*val),
								)
							} else {
								doc = bson.NewDocument(
									bson.EC.Int64("foo", val),
									bson.EC.Int64("mag", 10*val),
									bson.EC.Int64("neg", -1*val),
									bson.EC.Int64("dup", val),
									bson.EC.Int64("dub", 2*val),
								)
							}

							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if !doc.Equal(docs[idx]) {
								grip.Infoln(idx, "src:", test.dataset[idx])
								grip.Infoln(idx, "in: ", docs[idx].ToExtJSON(false))
								grip.Infoln(idx, "out:", doc.ToExtJSON(false))
							}
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res), "%v -> %v", test.dataset, res)
						require.Equal(t, len(test.dataset), len(res))
					})
				})
			}
		})
	}
}

func TestFixedEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	for _, impl := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 20} },
		},
		{
			name:    "StableDynamic",
			factory: func() Collector { return NewDynamicCollector(100) },
		},
		{
			name:    "Streaming",
			factory: func() Collector { return newStreamingCollector(20, &bytes.Buffer{}) },
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector := impl.factory()
						for _, val := range test.dataset {
							assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("foo", val))))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							assert.Equal(t, val, test.dataset[idx])
							idx++
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						if !assert.Equal(t, test.dataset, res) {
							grip.Infoln("in:", test.dataset)
							grip.Infoln("out:", res)
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector := impl.factory()
						docs := []*bson.Document{}

						for _, val := range test.dataset {
							doc := bson.NewDocument(
								bson.EC.Int64("foo", val),
								bson.EC.Int64("dub", 2*val),
								bson.EC.Int64("dup", val),
								bson.EC.Int64("neg", -1*val),
								bson.EC.Int64("mag", 10*val),
							)
							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if !doc.Equal(docs[idx]) {
								grip.Infoln(idx, "src:", test.dataset[idx])
								grip.Infoln(idx, "in: ", docs[idx].ToExtJSON(false))
								grip.Infoln(idx, "out:", doc.ToExtJSON(false))
							}
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						assert.Equal(t, test.dataset, res)
					})
				})
			}
			t.Run("SizeMismatch", func(t *testing.T) {
				collector := impl.factory()
				assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5))))
				assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5))))

				if strings.Contains(impl.name, "Dynamic") {
					assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43))))
				} else {
					assert.Error(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43))))
				}
			})
		})
	}
}

func TestCollectorSizeCap(t *testing.T) {
	for _, test := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 1} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := test.factory()
			assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5))))
			assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5))))
			assert.Error(t, collector.Add(bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5))))
		})
	}
}

func TestWriter(t *testing.T) {
	t.Run("NilDocuments", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		_, err := collector.Write(nil)
		assert.Error(t, err)
	})
	t.Run("RealDocument", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		doc, err := bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5)).MarshalBSON()
		require.NoError(t, err)
		_, err = collector.Write(doc)
		assert.NoError(t, err)
	})
	t.Run("CloseNoError", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		assert.NoError(t, collector.Close())
	})
	t.Run("CloseError", func(t *testing.T) {
		collector := NewWriterCollector(2, &errWriter{})
		doc, err := bson.NewDocument(bson.EC.Int64("one", 43), bson.EC.Int64("two", 5)).MarshalBSON()
		require.NoError(t, err)
		_, err = collector.Write(doc)
		require.NoError(t, err)
		assert.Error(t, collector.Close())
	})
}
