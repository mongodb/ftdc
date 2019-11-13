package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPerformanceType(t *testing.T) {
	t.Run("MethodsPanicWhenNil", func(t *testing.T) {
		var perf *Performance
		assert.Nil(t, perf)
		assert.Panics(t, func() {
			perf.MarshalDocument()
		})
		assert.Panics(t, func() {
			_, err := perf.MarshalBSON()
			assert.Error(t, err)
		})
		assert.Panics(t, func() {
			perf.Add(nil)
		})
		assert.Panics(t, func() {
			perf = &Performance{}
			perf.Add(nil)
		})
	})
	t.Run("Document", func(t *testing.T) {
		perf := &Performance{}
		doc := perf.MarshalDocument()
		assert.NotNil(t, doc)
		assert.Equal(t, 5, doc.Len())
	})
	t.Run("BSON", func(t *testing.T) {
		perf := &Performance{}
		out, err := perf.MarshalBSON()
		assert.NoError(t, err)
		assert.NotNil(t, out)
	})
	t.Run("Add", func(t *testing.T) {
		t.Run("Zero", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{})
			assert.Equal(t, &Performance{}, perf)
		})
		t.Run("OverridesID", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{ID: 100})
			assert.EqualValues(t, 100, perf.ID)
			perf.Add(&Performance{ID: 100})
			assert.EqualValues(t, 100, perf.ID)
		})
		t.Run("Counter", func(t *testing.T) {
			perf := &Performance{}
			perf.Add(&Performance{Counters: PerformanceCounters{Number: 100}})
			assert.EqualValues(t, 100, perf.Counters.Number)
			perf.Add(&Performance{Counters: PerformanceCounters{Number: 100}})
			assert.EqualValues(t, 200, perf.Counters.Number)
		})
	})
}
