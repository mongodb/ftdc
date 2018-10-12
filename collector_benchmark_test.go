package ftdc

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkCollectorInterface(b *testing.B) {
	collectors := createCollectors()
	for _, collect := range collectors {
		b.Run(collect.name, func(b *testing.B) {
			tests := createTests()
			for _, test := range tests {
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
					var result []byte
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
