package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/loki/v3/pkg/dataobj/arrowflight"
	"github.com/grafana/loki/v3/pkg/engine"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// StoreFlightSQL names the store that answers LogQL by translating it to SQL
// and running it through DataFusion against the Arrow Flight server.
const StoreFlightSQL = "dataobj-flight-sql"

// FlightSQLStore serves the generated data objects over Arrow Flight from
// this process and queries them through the DataFusion CLI in
// tools/dataobj-sql, which it keeps running in serve mode so process startup
// is paid once per store rather than once per query.
type FlightSQLStore struct {
	logger log.Logger
	server flight.Server
	logs   *arrowflight.TableSchema

	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
}

// NewFlightSQLStore opens the data objects under dir, starts a Flight server
// on a loopback port and launches dataobj-sql against it.
func NewFlightSQLStore(dir string, logger log.Logger) (*FlightSQLStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx := context.Background()

	bucket, err := filesystem.NewBucket(filepath.Join(dir, storageDir, "dataobj"))
	if err != nil {
		return nil, fmt.Errorf("opening bucket: %w", err)
	}
	catalog, err := arrowflight.OpenCatalog(ctx, bucket, "objects", logger)
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	logsSchema, ok := catalog.Table(arrowflight.TableLogs)
	if !ok {
		return nil, errors.New("catalog has no logs table")
	}

	srv := flight.NewServerWithMiddleware(nil)
	if err := srv.Init("127.0.0.1:0"); err != nil {
		return nil, fmt.Errorf("starting flight server: %w", err)
	}
	srv.RegisterFlightService(arrowflight.NewServer(catalog, logger))
	go func() { _ = srv.Serve() }()

	bin, err := dataobjSQLBinary(logger)
	if err != nil {
		srv.Shutdown()
		return nil, err
	}

	cmd := exec.Command(bin, "--serve", "--addr", "http://"+srv.Addr().String())
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		srv.Shutdown()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		srv.Shutdown()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		srv.Shutdown()
		return nil, fmt.Errorf("starting %s: %w", bin, err)
	}

	store := &FlightSQLStore{
		logger: logger,
		server: srv,
		logs:   logsSchema,
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReaderSize(stdout, 1<<20),
	}

	// Warm up the gRPC connection and DataFusion's planner so the first
	// measured query does not pay for process and connection startup, which
	// the other stores pay in their constructors.
	if _, err := store.run("SELECT count(*) FROM streams"); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("warm-up query failed: %w", err)
	}

	level.Info(logger).Log("msg", "flight sql store ready", "flight_addr", srv.Addr(), "dataobj_sql", bin)
	return store, nil
}

// dataobjSQLBinary locates the dataobj-sql binary, honouring DATAOBJ_SQL_BIN
// and otherwise building the crate in tools/dataobj-sql with cargo.
func dataobjSQLBinary(logger log.Logger) (string, error) {
	if p := os.Getenv("DATAOBJ_SQL_BIN"); p != "" {
		return p, nil
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot determine source location to find tools/dataobj-sql")
	}
	crate := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "tools", "dataobj-sql")

	release := filepath.Join(crate, "target", "release", "dataobj-sql")
	if _, err := os.Stat(release); err == nil {
		return release, nil
	}

	level.Info(logger).Log("msg", "building dataobj-sql with cargo; this takes a few minutes the first time", "dir", crate)
	build := exec.Command("cargo", "build", "--release", "--quiet")
	build.Dir = crate
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("building dataobj-sql (set DATAOBJ_SQL_BIN to skip): %w", err)
	}
	return release, nil
}

// Name implements Store.
func (s *FlightSQLStore) Name() string { return StoreFlightSQL }

// Engine returns a logql.Engine that answers queries through SQL.
func (s *FlightSQLStore) Engine() logql.Engine { return flightSQLEngine{store: s} }

// Close stops dataobj-sql and the Flight server.
func (s *FlightSQLStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = s.stdin.Close()
	err := s.cmd.Wait()
	s.server.Shutdown()
	return err
}

type sqlResponse struct {
	Rows      []map[string]json.RawMessage `json:"rows"`
	RowCount  int                          `json:"row_count"`
	ElapsedMs float64                      `json:"elapsed_ms"`
	WireBytes int64                        `json:"wire_bytes"`
	Error     string                       `json:"error"`
}

// run sends one statement to dataobj-sql and waits for its response.
func (s *FlightSQLStore) run(sql string) (*sqlResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req, err := json.Marshal(struct {
		SQL string `json:"sql"`
	}{sql})
	if err != nil {
		return nil, err
	}
	if _, err := s.stdin.Write(append(req, '\n')); err != nil {
		return nil, fmt.Errorf("writing to dataobj-sql: %w", err)
	}

	line, err := s.stdout.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("reading from dataobj-sql: %w", err)
	}

	var resp sqlResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("decoding dataobj-sql response: %w", err)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}

type flightSQLEngine struct{ store *FlightSQLStore }

func (e flightSQLEngine) Query(p logql.Params) logql.Query {
	return &flightSQLQuery{store: e.store, params: p}
}

type flightSQLQuery struct {
	store  *FlightSQLStore
	params logql.Params
}

// Exec translates the LogQL query to SQL, runs it, and converts the rows
// back. Queries outside the translatable subset fail with
// engine.ErrNotSupported so the bench harness skips them.
func (q *flightSQLQuery) Exec(_ context.Context) (logqlmodel.Result, error) {
	plan, err := translateLogQL(q.params, q.store.logs)
	if errors.Is(err, errSQLUnsupported) {
		return logqlmodel.Result{}, fmt.Errorf("%w: %v", engine.ErrNotSupported, err)
	} else if err != nil {
		return logqlmodel.Result{}, err
	}

	start := time.Now()
	resp, err := q.store.run(plan.SQL)
	if err != nil {
		return logqlmodel.Result{}, fmt.Errorf("executing %q: %w", plan.SQL, err)
	}

	var data parser.Value
	switch plan.Kind {
	case sqlPlanLogs:
		data, err = plan.logsResult(resp.Rows)
	case sqlPlanMetric:
		data, err = plan.metricResult(resp.Rows)
	}
	if err != nil {
		return logqlmodel.Result{}, err
	}

	level.Debug(q.store.logger).Log("msg", "flight sql query",
		"logql", q.params.QueryString(), "sql", plan.SQL,
		"rows", resp.RowCount, "wire_bytes", resp.WireBytes, "datafusion_ms", resp.ElapsedMs,
		"notes", strings.Join(plan.Notes, "; "))

	var result logqlmodel.Result
	result.Data = data
	// Surface which parts DataFusion evaluated itself; the comparison ignores
	// warnings but the report prints them.
	result.Warnings = plan.Notes
	result.Statistics.Summary.ExecTime = time.Since(start).Seconds()
	// There is no "bytes processed" on this path; report what crossed the wire.
	result.Statistics.Summary.TotalBytesProcessed = resp.WireBytes
	result.Statistics.Summary.TotalEntriesReturned = int64(resp.RowCount)
	return result, nil
}
