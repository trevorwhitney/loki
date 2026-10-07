# k6 with the SQL extension and its Postgres driver, for running the
# query-in-place measurement matrix (tools/sqlmeasure/k6/matrix.js) inside a
# cell: SQL goes straight to the dataobj-sql gateway over pgwire (no Grafana
# query timeout in the way), LogQL to the query-frontend over HTTP.
#
#   docker build -f tools/sqlmeasure/k6/Dockerfile -t <registry>/k6-sqlmeasure:<tag> tools/sqlmeasure/k6
FROM grafana/xk6:latest AS build
RUN xk6 build v2.3.0 \
    --with github.com/grafana/xk6-sql@latest \
    --with github.com/grafana/xk6-sql-driver-postgres@latest \
    --output /tmp/k6

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /tmp/k6 /usr/bin/k6
COPY matrix.js /scripts/matrix.js
ENTRYPOINT ["/usr/bin/k6"]
