package ftdc

import (
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

// this is an attempt to do the right thing for the collector, porting
// directly from the server implementation
type betterCollector struct {
	reference  *bson.Document
	metadata   *bson.Document
	lastSample []int64
	numSamples int
}

func (c *betterCollector) SetMetadata(doc *bson.Document) { c.metadata = doc }
func (c *betterCollector) Add(doc *bson.Document) error {
	if c.reference == nil {
		c.reference = doc
		sample, err := extractMetricsFromDocument(doc)
		if err != nil {
			return errors.WithStack(err)
		}
		c.lastSample = sample
		return
	}

}
