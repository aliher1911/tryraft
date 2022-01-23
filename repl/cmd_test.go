package repl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	c := ParseCmd("bla")

	require.Equal(t, "bla", c.Cmd)
}

func TestPositionalParams(t *testing.T) {
	c := ParseCmd("bla 7 3 19")

	for i, v := range []uint64{7, 3, 19} {
		val, err := c.GetUInt(i)
		require.NoError(t, err)
		require.Equal(t, v, val)
	}
}

func TestNamedParams(t *testing.T) {
	c := ParseCmd("bla id1=7 id2=5")

	for k, v := range map[string]uint64{"id1": 7, "id2": 5} {
		val, err := c.GetUInt(k)
		require.NoError(t, err)
		require.Equal(t, v, val)
	}
}

func TestNamedListParams(t *testing.T) {
	c := ParseCmd("bla id1=7,5 id2=13 id2=19")

	for k, v := range map[string][]uint64{"id1": {7, 5}, "id2": {13, 19}} {
		val, err := c.GetUIntSliceOr(k, nil)
		require.NoError(t, err)
		require.Equal(t, v, val)
	}
}

func TestDefaults(t *testing.T) {
	c := ParseCmd("bla")

	v, err := c.GetUIntOr("meow", 7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), v)

	dv := []uint64{1, 7, 13}
	v1, err := c.GetUIntSliceOr("woof", dv)
	require.NoError(t, err)
	require.Equal(t, dv, v1)
}
