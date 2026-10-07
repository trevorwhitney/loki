package arrowflight

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

// stubRing records the keys it was asked for and answers with a fixed
// replication set, or a fixed error.
type stubRing struct {
	keys      []uint32
	instances []ring.InstanceDesc
	err       error
}

func (s *stubRing) Get(key uint32, _ ring.Operation, _ []ring.InstanceDesc, _, _ []string) (ring.ReplicationSet, error) {
	s.keys = append(s.keys, key)
	if s.err != nil {
		return ring.ReplicationSet{}, s.err
	}
	return ring.ReplicationSet{Instances: s.instances}, nil
}

func TestRingLocator_Locate(t *testing.T) {
	ctx := context.Background()

	t.Run("locations carry the scheme and the instance addresses in ring order", func(t *testing.T) {
		r := &stubRing{instances: []ring.InstanceDesc{
			{Addr: "10.0.0.1:9095"},
			{Addr: "10.0.0.2:9095"},
			{Addr: ""}, // an instance without an address cannot be a location
		}}
		l := NewRingLocator(r, SchemeGRPCTLS, nil)

		locs, err := l.Locate(ctx, &scanpb.Ticket{ObjectPath: "objects/ab/one"})
		require.NoError(t, err)
		require.Equal(t, []string{"grpc+tls://10.0.0.1:9095", "grpc+tls://10.0.0.2:9095"}, locs)
	})

	t.Run("defaults to plain gRPC", func(t *testing.T) {
		r := &stubRing{instances: []ring.InstanceDesc{{Addr: "host:1"}}}
		locs, err := NewRingLocator(r, "", nil).Locate(ctx, &scanpb.Ticket{ObjectPath: "x"})
		require.NoError(t, err)
		require.Equal(t, []string{"grpc+tcp://host:1"}, locs)
	})

	t.Run("sections of one object hash to the same token", func(t *testing.T) {
		r := &stubRing{instances: []ring.InstanceDesc{{Addr: "host:1"}}}
		l := NewRingLocator(r, "", nil)
		for i := range int32(3) {
			_, err := l.Locate(ctx, &scanpb.Ticket{ObjectPath: "objects/ab/one", SectionIndex: i})
			require.NoError(t, err)
		}
		_, err := l.Locate(ctx, &scanpb.Ticket{ObjectPath: "objects/ab/two"})
		require.NoError(t, err)

		require.Len(t, r.keys, 4)
		require.Equal(t, r.keys[0], r.keys[1])
		require.Equal(t, r.keys[0], r.keys[2])
		require.NotEqual(t, r.keys[0], r.keys[3], "a different object should land on a different token")
	})

	t.Run("an empty ring yields no locations rather than an error", func(t *testing.T) {
		r := &stubRing{err: ring.ErrEmptyRing}
		locs, err := NewRingLocator(r, "", nil).Locate(ctx, &scanpb.Ticket{ObjectPath: "x"})
		require.NoError(t, err)
		require.Empty(t, locs)

		r = &stubRing{err: errors.New("too many unhealthy instances")}
		locs, err = NewRingLocator(r, "", nil).Locate(ctx, &scanpb.Ticket{ObjectPath: "x"})
		require.NoError(t, err)
		require.Empty(t, locs)
	})
}
