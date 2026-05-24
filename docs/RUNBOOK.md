# QuantStream Runbook

## Starting the Platform

```bash
make up
```

This starts all services: Redpanda, MinIO, Spark (master + worker), Prometheus, Grafana.

Wait ~30 seconds for all services to be healthy.

## Stopping the Platform

```bash
make down
```

## Service Endpoints

| Service | URL | Purpose |
|---|---|---|
| Redpanda Admin | http://localhost:19644 | Broker health, topic management |
| MinIO Console | http://localhost:9001 | Browse stored Parquet files |
| Spark Master UI | http://localhost:8080 | Monitor Spark jobs |
| FastAPI | http://localhost:8000 | API endpoints |
| FastAPI Docs | http://localhost:8000/docs | Swagger UI |
| Prometheus | http://localhost:9090 | Metrics exploration |
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |

## Health Checks

### Full system health
```bash
curl http://localhost:8000/health
```

### Redpanda health
```bash
curl http://localhost:19644/v1/status/node
```

### MinIO health
```bash
curl http://localhost:9000/minio/health/live
```

## Common Issues

### Redpanda won't start
- Check port conflicts: `lsof -i :19092`
- Check Docker logs: `docker logs quantstream-redpanda`
- Restart: `docker compose restart redpanda`

### Topics not created
- Run topic creation script: `bash scripts/create_topics.sh`
- Check Redpanda is healthy first
- Verify init container finished: `docker logs quantstream-redpanda-init`

### Spark can't connect to MinIO
- Verify MinIO is healthy
- Check S3A configuration in Spark job
- Verify bucket exists: check MinIO console at http://localhost:9001

### API returns 500 errors
- Check DuckDB can read Parquet files
- Verify data paths in environment config
- Check API logs: `docker logs <api-container>`

### No data in API responses
- Verify ingestion producer is running and connected
- Check Redpanda topic has messages: `docker exec quantstream-redpanda rpk topic describe raw.trades`
- Verify Spark pipeline is running

## Restarting from Scratch

```bash
make down
make clean-data
docker compose up -d
```

## Monitoring Alerts

### Consumer Lag High (>120s)
- Check Spark consumer is running
- Check Redpanda topic throughput
- Consider scaling Spark workers

### Ingestion Connection Down
- Check Binance API status
- Check network connectivity
- Review ingestion producer logs

### Dead Letter Rate High
- Investigate DLQ topic for failure patterns
- Check for schema changes or malformed upstream data
- Review silver validation rules
