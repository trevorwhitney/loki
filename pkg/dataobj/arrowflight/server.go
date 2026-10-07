package arrowflight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight/scanpb"
)

// Server implements the Arrow Flight service over one or more [Source]s.
//
// Supported calls:
//
//   - ListFlights lists the served tables.
//   - GetSchema returns the schema of a table; the descriptor is either a path
//     of one element (the table name) or a [scanpb.ScanRequest] command.
//   - GetFlightInfo plans a [scanpb.ScanRequest] command into one endpoint per
//     ticket returned by the owning source. With a [Locator], every endpoint
//     also names the servers that should serve it.
//   - DoGet streams the record batches of one endpoint.
type Server struct {
	flight.BaseFlightServer

	sources []Source
	byTable map[string]Source
	logger  log.Logger
	alloc   memory.Allocator
	metrics *Metrics
	locator Locator
	scanSem *semaphore.Weighted
}

// NewServer returns a Flight service serving the tables of every source with
// unregistered metrics; see [NewServerWithMetrics].
func NewServer(logger log.Logger, sources ...Source) *Server {
	return NewServerWithMetrics(logger, nil, sources...)
}

// NewServerWithMetrics returns a Flight service serving the tables of every
// source. Table names must be unique across sources. Register it with
// [flight.Server.RegisterFlightService].
func NewServerWithMetrics(logger log.Logger, metrics *Metrics, sources ...Source) *Server {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if metrics == nil {
		metrics = NewMetrics(nil)
	}
	s := &Server{
		sources: sources,
		byTable: make(map[string]Source),
		logger:  logger,
		alloc:   memory.DefaultAllocator,
		metrics: metrics,
	}
	for _, src := range sources {
		for _, name := range src.Tables() {
			s.byTable[name] = src
		}
	}
	return s
}

// WithMaxConcurrentScans bounds the DoGet calls served at once; further
// calls wait for a slot (or their context to end). Clients such as
// DataFusion open one DoGet per planned endpoint at the same time, so without
// a bound a wide scan opens every section at once and memory grows with the
// plan rather than with the server. 0 or less leaves scans unbounded.
// Returns s for chaining.
func (s *Server) WithMaxConcurrentScans(n int) *Server {
	if n > 0 {
		s.scanSem = semaphore.NewWeighted(int64(n))
	} else {
		s.scanSem = nil
	}
	return s
}

// WithLocator makes GetFlightInfo put the locations returned by l on every
// endpoint. Without a locator endpoints carry no locations and clients read
// them from the server they planned on. Returns s for chaining.
func (s *Server) WithLocator(l Locator) *Server {
	s.locator = l
	return s
}

// Tables returns the names of all served tables in a stable order.
func (s *Server) Tables() []string {
	var names []string
	for _, src := range s.sources {
		names = append(names, src.Tables()...)
	}
	return names
}

func (s *Server) table(name string) (Source, *TableSchema, error) {
	src, ok := s.byTable[name]
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "unknown table %q", name)
	}
	ts, ok := src.Table(name)
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "unknown table %q", name)
	}
	return src, ts, nil
}

// ListFlights implements [flight.FlightServer].
func (s *Server) ListFlights(_ *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	for _, name := range s.Tables() {
		_, ts, err := s.table(name)
		if err != nil {
			return err
		}
		info := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(ts.Schema, s.alloc),
			FlightDescriptor: &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{name}},
			TotalRecords:     -1,
			TotalBytes:       -1,
		}
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

// GetSchema implements [flight.FlightServer].
func (s *Server) GetSchema(_ context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	req, err := scanRequestFromDescriptor(desc)
	if err != nil {
		return nil, err
	}
	_, ts, err := s.table(req.GetTable())
	if err != nil {
		return nil, err
	}
	schema, err := ts.Project(req.GetColumns())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.alloc)}, nil
}

// GetFlightInfo implements [flight.FlightServer].
func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	req, err := scanRequestFromDescriptor(desc)
	if err != nil {
		return nil, err
	}
	src, ts, err := s.table(req.GetTable())
	if err != nil {
		return nil, err
	}
	schema, err := ts.Project(req.GetColumns())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	planStart := time.Now()
	tickets, err := src.Plan(ctx, req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.metrics.planDuration.WithLabelValues(req.GetTable()).Observe(time.Since(planStart).Seconds())
	s.metrics.plannedTickets.WithLabelValues(req.GetTable()).Observe(float64(len(tickets)))

	endpoints := make([]*flight.FlightEndpoint, 0, len(tickets))
	var located int
	for _, tkt := range tickets {
		raw, err := proto.Marshal(tkt)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encoding ticket: %v", err)
		}
		ep := &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: raw}}
		if s.locator != nil {
			locs, err := s.locator.Locate(ctx, tkt)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "locating ticket for %s: %v", tkt.GetObjectPath(), err)
			}
			for _, uri := range locs {
				ep.Location = append(ep.Location, &flight.Location{Uri: uri})
			}
			if len(locs) > 0 {
				located++
			}
		}
		endpoints = append(endpoints, ep)
	}
	s.metrics.locatedEndpoints.WithLabelValues(req.GetTable()).Add(float64(located))

	level.Debug(s.logger).Log("msg", "planned scan", "table", req.GetTable(), "columns", len(req.GetColumns()), "predicates", describePredicates(req.GetPredicates()), "endpoints", len(endpoints), "located", located)

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: desc,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGet implements [flight.FlightServer].
func (s *Server) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	var t scanpb.Ticket
	if err := proto.Unmarshal(tkt.GetTicket(), &t); err != nil {
		return status.Errorf(codes.InvalidArgument, "decoding ticket: %v", err)
	}

	ctx := stream.Context()
	start := time.Now()
	table := t.GetRequest().GetTable()

	src, _, err := s.table(table)
	if err != nil {
		return err
	}
	if s.scanSem != nil {
		queued := time.Now()
		if err := s.scanSem.Acquire(ctx, 1); err != nil {
			return status.Errorf(codes.Canceled, "waiting for a scan slot: %v", err)
		}
		defer s.scanSem.Release(1)
		s.metrics.scanQueueTime.Observe(time.Since(queued).Seconds())
	}
	s.metrics.scansInFlight.Inc()
	defer s.metrics.scansInFlight.Dec()

	sc, err := src.Scan(ctx, &t)
	if err != nil {
		s.metrics.scanErrors.WithLabelValues(table).Inc()
		return status.Errorf(codes.InvalidArgument, "opening scan: %v", err)
	}
	defer sc.Close()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(sc.Schema()))

	var rows, batches, bytes int64
	for {
		rec, err := sc.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			_ = w.Close()
			s.metrics.scanErrors.WithLabelValues(table).Inc()
			return status.Errorf(codes.Internal, "reading %s: %v", t.GetObjectPath(), err)
		}

		werr := w.Write(rec)
		rows += rec.NumRows()
		batches++
		bytes += recordBytes(rec)
		rec.Release()
		if werr != nil {
			_ = w.Close()
			s.metrics.scanErrors.WithLabelValues(table).Inc()
			return fmt.Errorf("writing record batch: %w", werr)
		}
	}

	if err := w.Close(); err != nil {
		s.metrics.scanErrors.WithLabelValues(table).Inc()
		return fmt.Errorf("closing record writer: %w", err)
	}
	s.metrics.scanDuration.WithLabelValues(table).Observe(time.Since(start).Seconds())
	s.metrics.rowsServed.WithLabelValues(table).Add(float64(rows))
	s.metrics.batchesServed.WithLabelValues(table).Add(float64(batches))
	s.metrics.bytesServed.WithLabelValues(table).Add(float64(bytes))

	objects := len(t.GetObjectPaths())
	if objects == 0 {
		objects = 1
	}
	level.Debug(s.logger).Log("msg", "served scan", "table", table, "object", t.GetObjectPath(), "objects", objects, "section", t.GetSectionIndex(), "rows", rows, "batches", batches, "bytes", bytes, "duration", time.Since(start))
	return nil
}

// scanRequestFromDescriptor decodes the scan request carried by a descriptor.
// A PATH descriptor of one element names a table and selects all columns.
func scanRequestFromDescriptor(desc *flight.FlightDescriptor) (*scanpb.ScanRequest, error) {
	switch desc.GetType() {
	case flight.DescriptorPATH:
		if len(desc.GetPath()) != 1 {
			return nil, status.Errorf(codes.InvalidArgument, "expected a path of one element naming a table, got %v", desc.GetPath())
		}
		return &scanpb.ScanRequest{Table: desc.GetPath()[0]}, nil

	case flight.DescriptorCMD:
		var req scanpb.ScanRequest
		if err := proto.Unmarshal(desc.GetCmd(), &req); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "decoding scan request: %v", err)
		}
		return &req, nil
	}

	return nil, status.Errorf(codes.InvalidArgument, "unsupported descriptor type %s", desc.GetType())
}
