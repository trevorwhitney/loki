.PHONY: generate bench list run run-debug stream grafana grafana-stop grafana-sql discover remote-test generate-k6 gotestsum-remote build-correctness-metrics flight-server sql compare dataobj-sql sql-server sql-server-stop

# Load local secrets from .env (ignored by git).
# Copy .env.template → .env and fill in real values.
-include .env
export

# Default size for data generation (2GB)
SIZE ?= 2147483648
TENANT ?= test-tenant

# Discover configuration — override via .env or environment
DISCOVER_MAX_STREAMS ?= 500
DISCOVER_CONCURRENCY ?= 5

# gotestsum and correctness-metrics configuration
GOTESTSUM ?= $(shell which gotestsum 2>/dev/null)
RANGE_TYPE ?= range
METADATA_DIR ?= testdata

# ARCHIVE_RULES is a semicolon-separated list of stream selectors; matching
# streams are written to data/archive as gzipped OTLP JSON (the archive-and-
# replay layout) instead of to the live stores. Example:
#   make generate ARCHIVE_RULES='{service_name="nginx"};{service_name="web-server", env="dev"}'
ARCHIVE_RULES ?=
generate:
	@echo "Generating benchmark data..."
	@go run ./cmd/generate/main.go -size $(SIZE) -tenant $(TENANT) -archive-rules '$(ARCHIVE_RULES)'

bench:
	@echo "Running all benchmarks..."
	@go test -bench=. -benchmem -count=1 -timeout=1h

list:
	@go run ./cmd/bench/main.go list

run:
	@go run ./cmd/bench/main.go run

run-debug:
	@DEBUG=1 go run ./cmd/bench/main.go run

stream:
	@go run ./cmd/stream/main.go | less

# Common docker flags for the Grafana targets: anonymous admin, no caching,
# the Loki datasource, and the Loki app plugins.
GRAFANA_DOCKER_ARGS = \
	--name loki-bench-grafana \
	-p 3000:3000 \
	-e GF_AUTH_ANONYMOUS_ENABLED=true \
	-e GF_AUTH_DISABLE_LOGIN_FORM=true \
	-e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
	-e GF_AUTH_BASIC_ENABLED=false \
	-e GF_USERS_DEFAULT_THEME=light \
	-e GF_CACHING_ENABLED=false \
	-e GF_PLUGINS_PREINSTALL_SYNC=grafana-lokiexplore-app@main@https://storage.googleapis.com/integration-artifacts/grafana-lokiexplore-app/release/main/any/grafana-lokiexplore-app-main.zip,grafana-lokioperational-app@main@https://storage.googleapis.com/integration-artifacts/grafana-lokioperational-app/release/latest/grafana-lokioperational-app-latest.zip \
	-v $(PWD)/grafana-datasource.yaml:/etc/grafana/provisioning/datasources/datasources.yml \
	-v $(PWD)/grafana-plugins.yaml:/etc/grafana/provisioning/plugins/app.yml

grafana:
	@echo "Starting Grafana with Loki datasource..."
	@docker run -d $(GRAFANA_DOCKER_ARGS) grafana/grafana:latest
	@echo "Grafana available at http://localhost:3000"
	@echo "Loki datasource pre-configured to connect to localhost:3100"

# grafana-sql is `grafana` plus a core Postgres datasource pointed at
# `make sql-server` (dataobj-sql speaking pgwire on $(PG_ADDR)) and the
# provisioned SQL dashboard, so LogQL and SQL panels sit side by side.
grafana-sql:
	@echo "Starting Grafana with Loki and dataobj-sql (Postgres) datasources..."
	@docker run -d $(GRAFANA_DOCKER_ARGS) \
		-v $(PWD)/grafana-datasource-sql.yaml:/etc/grafana/provisioning/datasources/datasources-sql.yml \
		-v $(PWD)/grafana-dashboards.yaml:/etc/grafana/provisioning/dashboards/dashboards.yml \
		-v $(PWD)/grafana-dashboard-sql.json:/var/lib/grafana/dashboards/dataobj-sql.json \
		-v $(PWD)/grafana-dashboard-archive.json:/var/lib/grafana/dashboards/dataobj-archive.json \
		grafana/grafana:latest
	@echo "Grafana available at http://localhost:3000"
	@echo "Postgres datasource 'dataobj-sql' connects to host.docker.internal:5432 (run 'make sql-server')"
	@echo "Dashboards: http://localhost:3000/d/dataobj-sql-bench and http://localhost:3000/d/dataobj-archive"

grafana-stop:
	@echo "Stopping Grafana container..."
	@docker stop loki-bench-grafana || true
	@docker rm loki-bench-grafana || true

loki-debug:
	@echo "Starting Loki on port 3100 with debug port 18001..."
	@(cd ../../../ && \
	make loki-debug && \
	dlv exec ./cmd/loki/loki-debug \
		--listen=:18001 \
		--headless=true \
		--api-version=2 \
		--accept-multiclient \
		--continue \
		-- -config.file=./pkg/logql/bench/config.yaml -config.expand-env=true -target=ui,dataobj-explorer,all)

loki:
	@echo "Starting Loki on port 3100 with debug port 18001..."
	@(cd ../../../ && \
		make loki && \
		cmd/loki/loki --config.file=./pkg/logql/bench/config.yaml -config.expand-env=true -target=ui,dataobj-explorer,all)

# server will start a local Loki instance and Grafana for interacting with bench data. You will need
# to manually kill the grafana process using `make grafana-stop` once you are done.
.PHONY: server
server: grafana loki

# server-debug will start a local Grafana and Loki with an exposed delve debug port, useful for
# interacting with bench data. The loki proceess will block, but can be killed using the DAP api,
# so this task defines a teardown command to kill grafana once loki is stopped.
.PHONY: server-debug
server-debug: grafana loki-debug

.PHONY: server-stop
server-stop:
	@make grafana-stop
	@pkill dlv
	@pkill loki

############
# Discover #
############

# Build the discover binary
discover/cmd/discover:
	CGO_ENABLED=0 go build -o $@ ./$(@D)

# discover runs the metadata discovery pipeline against the live Loki cluster.
# Requires secrets in .env (copy .env.template and fill in values):
#   LOKI_ADDR, LOKI_USERNAME, LOKI_PASSWORD     — Loki endpoint and auth
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY    — S3 credentials for TSDB index access
#   S3_BUCKET, S3_REGION, S3_ENDPOINT           — S3 bucket configuration
#   TABLE_PREFIX                                — TSDB index table name prefix
discover: discover/cmd/discover
ifndef LOKI_ADDR
	$(error LOKI_ADDR is required — add it to .env (see .env.template))
endif
ifndef LOKI_USERNAME
	$(error LOKI_USERNAME is required — add it to .env (see .env.template))
endif
ifndef LOKI_PASSWORD
	$(error LOKI_PASSWORD is required — add it to .env (see .env.template))
endif
ifndef AWS_ACCESS_KEY_ID
	$(error AWS_ACCESS_KEY_ID is required — add it to .env (see .env.template))
endif
ifndef AWS_SECRET_ACCESS_KEY
	$(error AWS_SECRET_ACCESS_KEY is required — add it to .env (see .env.template))
endif
ifndef S3_BUCKET
	$(error S3_BUCKET is required — add it to .env (see .env.template))
endif
ifndef S3_REGION
	$(error S3_REGION is required — add it to .env (see .env.template))
endif
ifndef S3_ENDPOINT
	$(error S3_ENDPOINT is required — add it to .env (see .env.template))
endif
ifndef TABLE_PREFIX
	$(error TABLE_PREFIX is required — add it to .env (see .env.template))
endif
	./discover/cmd/discover \
		--tenant $(LOKI_USERNAME) \
		--storage-type s3 \
		--s3.buckets "$(S3_BUCKET)" \
		--s3.region "$(S3_REGION)" \
		--s3.endpoint "$(S3_ENDPOINT)" \
		--s3.access-key-id "$(AWS_ACCESS_KEY_ID)" \
		--s3.secret-access-key "$(AWS_SECRET_ACCESS_KEY)" \
		--table-prefix "$(TABLE_PREFIX)" \
		--selector '$(DISCOVER_SELECTOR)' \
		--max-streams $(DISCOVER_MAX_STREAMS) \
		--concurrency $(DISCOVER_CONCURRENCY) \
		--output testdata \
		--queries-dir queries \
		$(if $(DISCOVER_FROM),--from "$(DISCOVER_FROM)") \
		$(if $(DISCOVER_TO),--to "$(DISCOVER_TO)")

###########################
# Remote Correctness Test #
###########################

# Default timeout for remote correctness tests
REMOTE_TIMEOUT ?= 30m

# remote-test compares query results between two live Loki endpoints.
# Requires:
#   1. Discovered metadata in testdata/ (run `make discover` first)
#   2. Secrets in .env (see .env.template):
#      REMOTE_ADDR_1, REMOTE_ADDR_2  — two Loki endpoints to compare
#      REMOTE_ORG_ID                 — tenant org ID
#      LOKI_USERNAME, LOKI_PASSWORD  — HTTP basic auth
remote-test:
ifndef REMOTE_ADDR_1
	$(error REMOTE_ADDR_1 is required — add it to .env (see .env.template))
endif
ifndef REMOTE_ADDR_2
	$(error REMOTE_ADDR_2 is required — add it to .env (see .env.template))
endif
ifndef REMOTE_ORG_ID
	$(error REMOTE_ORG_ID is required — add it to .env (see .env.template))
endif
	go test -tags=remote_correctness -v \
		. \
		-run TestRemoteStorageEquality \
		-timeout $(REMOTE_TIMEOUT) \
		-addr-1 "$(REMOTE_ADDR_1)" \
		-addr-2 "$(REMOTE_ADDR_2)" \
		-org-id "$(REMOTE_ORG_ID)" \
		-username "$(LOKI_USERNAME)" \
		-password "$(LOKI_PASSWORD)" \
		-metadata-dir testdata

#############################
# k6 Test Case Generation   #
#############################

TENANT_ID ?= 29

# generate-k6 resolves query registry templates against discovered metadata
# and outputs JSON files for the k6 Goldfish comparison test suite.
# Requires: discovered metadata in testdata/ (run `make discover` first)
generate-k6:
	go run ./cmd/generate-k6/main.go \
		--metadata-dir testdata \
		--queries-dir queries \
		--tenant-id $(TENANT_ID) \
		--output-dir testdata

#####################################
# Correctness Metrics Binary        #
#####################################

build-correctness-metrics:
	@mkdir -p ./build
	CGO_ENABLED=0 go build -o ./build/correctness-metrics ./cmd/correctness-metrics

#####################################
# gotestsum Remote Test Runner      #
#####################################

# gotestsum-remote runs the remote correctness tests via gotestsum,
# producing JUnit XML output in ./build/results.xml.
# Requires gotestsum in $PATH: go install gotest.tools/gotestsum@latest
gotestsum-remote:
ifndef GOTESTSUM
	$(error gotestsum not found in PATH — install with: go install gotest.tools/gotestsum@latest)
endif
ifndef REMOTE_ADDR_1
	$(error REMOTE_ADDR_1 is required — add it to .env (see .env.template))
endif
ifndef REMOTE_ADDR_2
	$(error REMOTE_ADDR_2 is required — add it to .env (see .env.template))
endif
ifndef REMOTE_ORG_ID
	$(error REMOTE_ORG_ID is required — add it to .env (see .env.template))
endif
	@mkdir -p ./build
	$(GOTESTSUM) \
		--format standard-verbose \
		--junitfile ./build/results.xml \
		-- -tags=remote_correctness -v \
		. \
		-run TestRemoteStorageEquality \
		-timeout $(REMOTE_TIMEOUT) \
		-addr-1 "$(REMOTE_ADDR_1)" \
		-addr-2 "$(REMOTE_ADDR_2)" \
		-org-id "$(REMOTE_ORG_ID)" \
		-username "$(LOKI_USERNAME)" \
		-password "$(LOKI_PASSWORD)" \
		-metadata-dir $(METADATA_DIR) \
		-remote-range-type $(RANGE_TYPE)

#####################################
# DataFusion SQL over data objects  #
#####################################

DATAOBJ_DIR ?= data/storage/dataobj
FLIGHT_ADDR ?= 127.0.0.1:8815

# Archive objects (gzipped OTLP JSON, archive-and-replay layout) served as the
# archive_logs table when ARCHIVE_DIR/ARCHIVE_TENANT exists. `make generate
# ARCHIVE_RULES=...` writes data/archive/$(TENANT); point ARCHIVE_DIR at a copy
# of a real bucket (for example archive-dev) and ARCHIVE_TENANT at a tenant id
# to serve real objects instead. ARCHIVE_LABELS lists the OTLP resource
# attributes promoted to labels for native OTLP objects (default: Loki's list).
ARCHIVE_DIR ?= data/archive
ARCHIVE_TENANT ?= $(TENANT)
ARCHIVE_LABELS ?=
ARCHIVE_FLAGS = $(if $(wildcard $(ARCHIVE_DIR)/$(ARCHIVE_TENANT)),-archive-dir $(ARCHIVE_DIR) -archive-tenant $(ARCHIVE_TENANT) $(if $(ARCHIVE_LABELS),-archive-labels '$(ARCHIVE_LABELS)',),)

# flight-server serves the generated data objects over Arrow Flight so that
# external query engines can scan them. Run `make generate` first.
flight-server:
	@echo "Serving $(DATAOBJ_DIR) over Arrow Flight on $(FLIGHT_ADDR)..."
	@go run ../../../tools/dataobj-flight -dir $(DATAOBJ_DIR) -addr $(FLIGHT_ADDR) $(ARCHIVE_FLAGS)

# sql runs a DataFusion SQL query against the running flight-server, e.g.
#   make sql QUERY="SELECT service_name, count(*) FROM logs GROUP BY 1"
# Requires a Rust toolchain and protoc; the first build takes a few minutes.
sql:
	@cd ../../../tools/dataobj-sql && cargo run --quiet --release -- --addr http://$(FLIGHT_ADDR) "$(QUERY)"

# dataobj-sql builds the Rust CLI (cargo is incremental, so this is a cheap
# no-op once built). Set DATAOBJ_SQL_BIN to use a binary built elsewhere.
DATAOBJ_SQL_DIR = ../../../tools/dataobj-sql
DATAOBJ_SQL_BIN ?= $(DATAOBJ_SQL_DIR)/target/release/dataobj-sql
dataobj-sql:
	@cd $(DATAOBJ_SQL_DIR) && cargo build --release --quiet

build/dataobj-flight: $(wildcard ../../../tools/dataobj-flight/*.go) $(wildcard ../../dataobj/arrowflight/*.go)
	@mkdir -p build
	@go build -o $@ ../../../tools/dataobj-flight

# sql-server runs the Flight server and dataobj-sql in Postgres wire protocol
# mode together in the foreground, logs interleaved. Point any Postgres client
# at $(PG_ADDR):
#   psql -h 127.0.0.1 -p 5432 -U loki -d datafusion -c 'SELECT count(*) FROM logs'
# Use FLIGHT_FLAGS=-v to log every scan the Flight server plans (shows which
# predicates were pushed down). Ctrl-C stops both; so does `make sql-server-stop`.
PG_ADDR ?= 0.0.0.0:5432
FLIGHT_FLAGS ?=
sql-server: build/dataobj-flight dataobj-sql
	@echo "Serving $(DATAOBJ_DIR) $(if $(ARCHIVE_FLAGS),and archive $(ARCHIVE_DIR)/$(ARCHIVE_TENANT) ,)over Arrow Flight on $(FLIGHT_ADDR) and Postgres wire protocol on $(PG_ADDR)..."
	@./build/dataobj-flight -dir $(DATAOBJ_DIR) -addr $(FLIGHT_ADDR) $(ARCHIVE_FLAGS) $(FLIGHT_FLAGS) & \
	FLIGHT_PID=$$!; \
	trap 'kill $$FLIGHT_PID 2>/dev/null' EXIT INT TERM; \
	until nc -z $(firstword $(subst :, ,$(FLIGHT_ADDR))) $(lastword $(subst :, ,$(FLIGHT_ADDR))) 2>/dev/null; do sleep 0.2; done; \
	$(DATAOBJ_SQL_BIN) --addr http://$(FLIGHT_ADDR) --pg-addr $(PG_ADDR) --tenant $(TENANT)

sql-server-stop:
	@pkill -f 'build/dataobj-flight' || true
	@pkill -f 'dataobj-sql.*--pg-addr' || true

# compare runs the storage-equality correctness test for the fast suite across
# the chunk baseline, the v2 engine, and the Flight/DataFusion POC, and writes
# a per-query latency report. Requires generated data (make generate) and a
# Rust toolchain (or DATAOBJ_SQL_BIN pointing at a built dataobj-sql).
REPORT ?= build/storage-equality-report.md
.PHONY: compare
compare:
	@mkdir -p $(dir $(REPORT))
	@go test -count=1 -v -timeout 2h -run 'TestStorageEquality/fast' . -slow-tests -flight-sql -report $(REPORT)
	@echo "Report written to $(REPORT)"
