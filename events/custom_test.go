package events

import (
	"testing"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	legacyBSON "gopkg.in/mgo.v2/bson"
)

func TestRollupRoundTrip(t *testing.T) {
	data := MakeCustom(3)
	assert.NoError(t, data.Add("a", 1.2))
	assert.NoError(t, data.Add("d", 100))
	assert.NoError(t, data.Add("b", 45.0))
	assert.Error(t, data.Add("foo", Custom{}))
	assert.Len(t, data, 3)

	t.Run("NewBSON", func(t *testing.T) {
		payload, err := bson.Marshal(data)
		require.NoError(t, err)

		rt := Custom{}
		err = bson.Unmarshal(payload, &rt)
		require.NoError(t, err)

		require.Len(t, rt, 3)
		assert.Equal(t, "a", rt[0].Name)
		assert.Equal(t, "b", rt[1].Name)
		assert.Equal(t, "d", rt[2].Name)
		assert.Equal(t, 1.2, rt[0].Value)
		assert.Equal(t, 45.0, rt[1].Value)
		assert.Equal(t, int64(100), rt[2].Value)
	})
	t.Run("LegacyBSON", func(t *testing.T) {
		payload, err := legacyBSON.Marshal(data)
		require.NoError(t, err)

		rt := Custom{}
		err = legacyBSON.Unmarshal(payload, &rt)
		require.NoError(t, err)

		require.Len(t, rt, 3)
		assert.Equal(t, "a", rt[0].Name)
		assert.Equal(t, "b", rt[1].Name)
		assert.Equal(t, "d", rt[2].Name)
		assert.Equal(t, 1.2, rt[0].Value)
		assert.Equal(t, 45.0, rt[1].Value)
		assert.Equal(t, 100, rt[2].Value)
	})
}
