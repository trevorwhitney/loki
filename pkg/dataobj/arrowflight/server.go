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
//     ticket returned by the owning source.
//   - DoGet streams the record batches of one endpoint.
type Server struct {
	flight.BaseFlightServer

	sources []Source
	byTable map[string]Source
	logger  log.Logger
	alloc   memory.Allocator
}

// NewServer returns a Flight service serving the tables of every source.
// Table names must be unique across sources. Register it with
// [flight.Server.RegisterFlightService].
func NewServer(logger log.Logger, sources ...Source) *Server {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	s := &Server{
		sources: sources,
		byTable: make(map[string]Source),
		logger:  logger,
		alloc:   memory.DefaultAllocator,
	}
	for _, src := range sources {
		for _, name := range src.Tables() {
			s.byTable[name] = src
		}
	}
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

	tickets, err := src.Plan(ctx, req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	endpoints := make([]*flight.FlightEndpoint, 0, len(tickets))
	for _, tkt := range tickets {
		raw, err := proto.Marshal(tkt)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encoding ticket: %v", err)
		}
		endpoints = append(endpoints, &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: raw}})
	}

	level.Debug(s.logger).Log("msg", "planned scan", "table", req.GetTable(), "columns", len(req.GetColumns()), "predicates", describePredicates(req.GetPredicates()), "endpoints", len(endpoints))

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

	src, _, err := s.table(t.GetRequest().GetTable())
	if err != nil {
		return err
	}
	sc, err := src.Scan(ctx, &t)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "opening scan: %v", err)
	}
	defer sc.Close()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(sc.Schema()))

	var rows, batches int64
	for {
		rec, err := sc.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			_ = w.Close()
			return status.Errorf(codes.Internal, "reading %s: %v", t.GetObjectPath(), err)
		}

		werr := w.Write(rec)
		rows += rec.NumRows()
		batches++
		rec.Release()
		if werr != nil {
			_ = w.Close()
			return fmt.Errorf("writing record batch: %w", werr)
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("closing record writer: %w", err)
	}

	objects := len(t.GetObjectPaths())
	if objects == 0 {
		objects = 1
	}
	level.Debug(s.logger).Log("msg", "served scan", "table", t.GetRequest().GetTable(), "object", t.GetObjectPath(), "objects", objects, "section", t.GetSectionIndex(), "rows", rows, "batches", batches, "duration", time.Since(start))
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
