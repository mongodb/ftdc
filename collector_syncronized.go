package ftdc

import (
	"sync"
)

type synchronizedCollector struct {
	Collector
	mu sync.RWMutex
}

func NewSynchronizedCollector(coll Collector) Collector {
	return &synchronizedCollector{
		Collector: coll,
	}
}

func (c *synchronizedCollector) Add(in interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Collector.Add(in)
}

func (c *synchronizedCollector) SetMetadata(in interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Collector.SetMetadata(in)
}

func (c *synchronizedCollector) Resolve() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Collector.Resolve()
}

func (c *synchronizedCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Collector.Reset()
}

func (c *synchronizedCollector) Info() CollectorInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.Collector.Info()
}
