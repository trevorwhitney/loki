# Builds the DataFusion SQL gateway (dataobj-sql). Build from the repository
# root because build.rs compiles pkg/dataobj/arrowflight/scanpb/scan.proto:
#
#   docker build -f tools/dataobj-sql/Dockerfile -t dataobj-sql:dev .
#
# The image serves the Postgres wire protocol in front of a Flight scan
# service, for example:
#
#   dataobj-sql --addr http://loki-dataobj-flight:9095 --tenant 29 --pg-addr 0.0.0.0:5432
FROM rust:1.90-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY pkg/dataobj/arrowflight/scanpb/scan.proto pkg/dataobj/arrowflight/scanpb/scan.proto
COPY tools/dataobj-sql tools/dataobj-sql
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/tools/dataobj-sql/target \
    cd tools/dataobj-sql && cargo build --release --locked \
    && cp target/release/dataobj-sql /dataobj-sql

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /dataobj-sql /usr/bin/dataobj-sql
EXPOSE 5432
ENTRYPOINT ["/usr/bin/dataobj-sql"]
