package events

import "github.com/mongodb/ftdc"

// EventsCollector wraps the ftdc.Collector interface and adds
// specific awareness of the Performance type from this package. These
// collectors should be responsible for cumulative summing of values,
// when appropriate.
//
// In general, implementations should obstruct calls to to underlying
// collectors Add() method to avoid confusion, either by panicing or
// by no-oping.
type EventsCollector interface {
	AddEvent(Performance) error
	ftdc.Collector
}

type basicCumulativeCollector struct {
	ftdc.Collector
	current *Performance
}

func (c *basicCumulativeCollector) Add(interface{}) error { return nil }

func (c *basicCumulativeCollector) AddEvent(in *Performance) error {
	if current. == nil {
		c.current = in
		return c.Collector.Add(c.current.MarshalDocument())
	}

	c.current.Add(in)
	return c.Collector.Add(c.current.MarshalDocument())
}

