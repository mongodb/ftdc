package events

import (
	"testing"

	"github.com/mongodb/ftdc"
	"github.com/stretchr/testify/assert"
)

func TestCollector(t *testing.T) {
	for _, fcTest := range []struct {
		name        string
		constructor func() ftdc.Collector
	}{
		{
			name: "Basic",
			constructor: func() ftdc.Collector {
				return ftdc.NewBasicCollector(100)
			},
		},
		{
			name: "Uncompressed",
			constructor: func() ftdc.Collector {
				return ftdc.NewUncompressedCollectorBSON(100)
			},
		},
		{
			name: "Dynamic",
			constructor: func() ftdc.Collector {
				return ftdc.NewDynamicCollector(100)
			},
		},
	} {
		t.Run(fcTest.name, func(t *testing.T) {
			for _, collectorTest := range []struct {
				name        string
				constructor func(ftdc.Collector) Collector
			}{
				{
					name: "Basic",
					constructor: func(fc ftdc.Collector) Collector {
						return NewBasicCollector(fc)
					},
				},
			} {
				t.Run(collectorTest.name, func(t *testing.T) {
					collector := collectorTest.constructor(fcTest.constructor())
					assert.NotNil(t, collector)
				})
			}
		})
	}
}
