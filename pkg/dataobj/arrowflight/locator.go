package arrowflight

import (
	"context"
	"hash/fnv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

// Locator decides which servers should serve a ticket. GetFlightInfo asks it
// for every planned ticket and puts the answer on the endpoint as Flight
// locations, so a client can fan DoGet calls out across a pool of servers
// instead of sending them all to the one that planned the scan.
//
// Locations are a routing hint: every server can serve every ticket, so a
// client that ignores them, or whose preferred location is unreachable, may
// send the ticket to any server it knows.
type Locator interface {
	// Locate returns the Flight location URIs for tkt, most preferred first.
	// An empty result leaves the endpoint without locations, which tells the
	// client to use the connection it planned on.
	Locate(ctx context.Context, tkt *scanpb.Ticket) ([]string, error)
}

// Flight location URI schemes, see
// https://arrow.apache.org/docs/format/Flight.html#connecting-to-flight-rpc-servers.
const (
	// SchemeGRPCTCP is plain gRPC over TCP.
	SchemeGRPCTCP = "grpc+tcp"
	// SchemeGRPCTLS is gRPC over TLS.
	SchemeGRPCTLS = "grpc+tls"
)

// ringReader is the part of [ring.ReadRing] a [RingLocator] uses. *ring.Ring
// implements it.
type ringReader interface {
	Get(key uint32, op ring.Operation, bufDescs []ring.InstanceDesc, bufHosts, bufZones []string) (ring.ReplicationSet, error)
}

// locateOp selects ACTIVE instances only; a server that is joining or
// leaving should not be handed scans.
var locateOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

// RingLocator places tickets on the members of a dskit ring. The object path
// of a ticket is hashed onto the ring, so every section of an object lands on
// the same replica set while the ring is stable, which keeps the per-process
// object cache of that replica warm. The replication factor of the ring is
// the number of locations offered per endpoint, most preferred first.
type RingLocator struct {
	ring   ringReader
	scheme string
	logger log.Logger
}

// NewRingLocator returns a locator over r. scheme is the Flight URI scheme
// servers are reachable with, [SchemeGRPCTCP] or [SchemeGRPCTLS].
func NewRingLocator(r ringReader, scheme string, logger log.Logger) *RingLocator {
	if scheme == "" {
		scheme = SchemeGRPCTCP
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &RingLocator{ring: r, scheme: scheme, logger: logger}
}

// Locate implements [Locator]. A ring without healthy instances yields no
// locations rather than an error, since the planning server can still serve
// the ticket itself.
func (l *RingLocator) Locate(_ context.Context, tkt *scanpb.Ticket) ([]string, error) {
	key := ticketKey(tkt)
	set, err := l.ring.Get(key, locateOp, nil, nil, nil)
	if err != nil {
		level.Debug(l.logger).Log("msg", "no ring placement for ticket; endpoint will have no locations", "object", tkt.GetObjectPath(), "err", err)
		return nil, nil
	}
	locs := make([]string, 0, len(set.Instances))
	for _, inst := range set.Instances {
		if inst.Addr == "" {
			continue
		}
		locs = append(locs, l.scheme+"://"+inst.Addr)
	}
	return locs, nil
}

// ticketKey hashes the object a ticket reads to a ring token. Tickets that
// bundle several objects (the archive) hash the first; the planner lists them
// in order, so consecutive bundles still spread across the ring.
func ticketKey(tkt *scanpb.Ticket) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tkt.GetObjectPath()))
	return h.Sum32()
}

// StaticLocator offers the same locations for every ticket. It is for
// deployments without a ring where the servers are known up front, and for
// tests.
type StaticLocator struct {
	Locations []string
}

// Locate implements [Locator].
func (l StaticLocator) Locate(context.Context, *scanpb.Ticket) ([]string, error) {
	return l.Locations, nil
}
