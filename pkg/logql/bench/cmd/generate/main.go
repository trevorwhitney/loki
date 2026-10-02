package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/bench"
)

func main() {
	var (
		size        = flag.Int64("size", 2147483648, "Size in bytes to generate")
		dir         = flag.String("dir", "data", "Output directory")
		tenantID    = flag.String("tenant", "test-tenant", "Tenant ID")
		clearFolder = flag.Bool("clear", true, "Clear output directory before generating data")
		archive     = flag.String("archive-rules", "", "Semicolon-separated LogQL stream selectors; matching streams are written as gzipped OTLP JSON archive objects under <dir>/archive/<tenant> instead of to the live stores, e.g. '{service_name=\"nginx\"};{service_name=\"web-server\", env=\"dev\"}'")
	)
	flag.Parse()

	// Clean the output directory if requested
	if *clearFolder {
		if err := os.RemoveAll(*dir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to clear output directory: %v\n", err)
			os.Exit(1)
		}
	}

	// Create stores
	chunkStore, err := bench.NewChunkStore(*dir, *tenantID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create chunk store: %v\n", err)
		os.Exit(1)
	}
	dataObjStore, err := bench.NewDataObjStore(*dir, *tenantID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create dataobj store: %v\n", err)
		os.Exit(1)
	}

	// Create builder with default options and the store
	builder := bench.NewBuilder(*dir, bench.DefaultOpt(), chunkStore, dataObjStore)

	if *archive != "" {
		archiveStore, err := bench.NewArchiveStore(*dir, *tenantID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create archive store: %v\n", err)
			os.Exit(1)
		}
		if err := builder.WithArchive(archiveStore, strings.Split(*archive, ";")); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid -archive-rules: %v\n", err)
			os.Exit(1)
		}
	}

	// Generate the data
	ctx := context.Background()
	if err := builder.Generate(ctx, *size); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate dataset: %v\n", err)
		os.Exit(1)
	}
}
