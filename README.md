# Real-Time Clickstream Lakehouse Pipeline

A end-to-end data engineering project where I built a real-time streaming pipeline that ingests e-commerce clickstream events, cleans and validates them, and aggregates them into business metrics — following the same bronze/silver/gold lakehouse pattern used in tools like Databricks and Palantir Foundry.

I built this to go deeper into data engineering concepts beyond just writing ETL scripts — things like streaming ingestion, schema enforcement, watermarking for late data, deduplication, and CDC-style upserts using Delta Lake.

## Why I built this

I've mostly worked on AI/backend projects (like [CodeGraph](https://github.com/shivprasad08), an AI-powered codebase visualizer), but I wanted a project that specifically demonstrates data engineering skills — the kind of pipeline architecture used in tools like Palantir Foundry. So instead of another CRUD app or ML model, I built something that focuses on how data moves and gets progressively cleaned as it flows through a system.

## What it actually does

1. A **producer** simulates a live e-commerce clickstream (page views, add-to-cart, purchases, etc.) and writes events as JSON files
2. **Spark Structured Streaming** picks up these files in real time
3. Data flows through three layers, each one cleaner than the last:
   - **Bronze** — raw data, exactly as it arrived, schema-enforced but untouched otherwise
   - **Silver** — validated, deduplicated, bad records filtered out and quarantined (not just dropped)
   - **Gold** — aggregated business metrics: rolling event counts and per-session revenue, updated using upserts (not just appends)
4. A **FastAPI** service exposes the Gold tables as REST endpoints so the data is actually queryable
5. (Optional) **Airflow** runs daily maintenance jobs (compaction + cleanup) on the Delta tables

Everything runs locally in Docker — no cloud account needed to try it out.

## Architecture

```
┌──────────┐     ┌─────────────┐     ┌────────┐     ┌────────┐     ┌────────┐     ┌─────────┐
│ Producer │────▶│ Raw JSON    │────▶│ Bronze │────▶│ Silver │────▶│  Gold  │────▶│ FastAPI │
│ (fake    │     │ files       │     │ (Delta)│     │ (Delta)│     │ (Delta)│     │  API    │
│ events)  │     │ (landing    │     │        │     │        │     │        │     │         │
└──────────┘     │  zone)      │     └────────┘     └────────┘     └────────┘     └─────────┘
                  └─────────────┘         │              │              │
                                          ▼              ▼              ▼
                                    quarantine       rejected      MinIO (S3-
                                    (bad JSON)     (bad records)   compatible
                                                                    storage)

                                    ┌──────────────────────────────┐
                                    │  Airflow (daily maintenance:  │
                                    │  OPTIMIZE + VACUUM on all     │
                                    │  Delta tables)                 │
                                    └──────────────────────────────┘
```

Each arrow above is a Spark Structured Streaming job running continuously, not a one-time batch script.

## Tech stack

| Layer | Tech |
|---|---|
| Stream processing | PySpark (Structured Streaming) |
| Storage format | Delta Lake |
| Object storage | MinIO (S3-compatible, local) |
| Serving layer | FastAPI + DuckDB |
| Orchestration | Airflow (for maintenance, not the streaming itself) |
| Containerization | Docker + Docker Compose |

I used **MinIO instead of AWS S3** so anyone (including me, testing this before an interview) can run the whole thing for free with no cloud account. Since MinIO speaks the same S3 API, swapping it for real AWS S3 later is just a config/endpoint change — no code changes needed in the Spark jobs.

## Project structure

```
clickstream-pipeline/
├── producer/          # simulates the clickstream, writes JSON events
├── ingestion/         # schema definition + streaming read logic
├── spark_jobs/        # bronze_layer.py, silver_layer.py, gold_layer.py
├── storage/           # local MinIO volume mount
├── api/               # FastAPI serving layer
├── orchestration/     # Airflow DAGs (maintenance jobs)
├── tests/             # unit tests per module
├── notebooks/         # scratch notebook for debugging
└── docker-compose.yml
```

## How to run it

```bash
git clone <this-repo>
cd clickstream-pipeline
cp .env.example .env
docker-compose up --build
```

Give it a minute or two to spin up (Spark takes a bit to start), then:

- MinIO console: `http://localhost:9001` (check the `clickstream-lakehouse` bucket is being populated)
- API docs (Swagger UI): `http://localhost:8000/docs`
- Airflow UI (if running): `http://localhost:8080`

To sanity check things are actually flowing, hit:
```
GET http://localhost:8000/metrics/event-counts
GET http://localhost:8000/metrics/sessions/top-revenue
```
and refresh a few times — the numbers should keep changing as new events stream in.

## Some design decisions I made (and why)

**Why keep Bronze "dirty"?**
It's tempting to clean data as soon as you read it, but I kept Bronze as an exact raw copy (just schema-enforced). This way, if my cleaning logic in Silver has a bug later, I still have the original data and can reprocess it — nothing is lost at the ingestion step.

**Why watermarking in Silver/Gold?**
Streaming data can arrive late or out of order. Watermarking tells Spark "wait up to 10 minutes for late data, then finalize this window." Without it, Spark would have to hold onto state forever, which isn't sustainable. The tradeoff: data arriving more than 10 minutes late gets dropped from aggregations. I think this is a reasonable tradeoff for this use case, but it's worth calling out as a limitation.

**Why MERGE INTO instead of just appending in Gold?**
Session metrics change as a session goes on (more events, more revenue) — appending would create duplicate rows per session. Using Delta's `MERGE INTO` (via `foreachBatch`) lets me update a session's row in place, which is closer to how real CDC (change data capture) pipelines work. This was actually the part I found most interesting to build, since it needed the `foreachBatch` pattern rather than a plain streaming writer.

**Why did I quarantine bad data instead of dropping it?**
The event generator intentionally injects a small percentage of bad records (missing fields, malformed timestamps, duplicates) to simulate real-world messy data. Instead of silently dropping these, I write them to separate `quarantine`/`rejected` tables. This felt closer to how a real data quality process should work — you want to know what's being filtered out and why, not just lose it.

## Known limitations (being honest about this)

- The API layer builds SQL queries using string interpolation for filters, which isn't safe for production (SQL injection risk). For this project's scope I documented it instead of over-engineering a fix, but in a real system I'd use parameterized queries.
- Airflow is running on SQLite + LocalExecutor, which is fine for a demo but not how you'd run Airflow in production (would need Postgres + a real executor).
- Kafka isn't used — I went with a simpler file-based streaming source to keep infrastructure manageable given my timeline. The architecture is designed so Kafka could be swapped in later without changing the Bronze/Silver/Gold logic.
- Watermarking means data arriving later than 10 minutes gets dropped from windowed aggregations — acceptable for this demo, but something a production system would need to handle more carefully depending on the use case.
- I didn't deploy this to real AWS since MinIO already proves the pattern; if I get more time I'd like to actually test the S3 swap.

## What I'd add if I had more time

- Swap file-based ingestion for actual Kafka
- Add proper authentication on the API
- Deploy the storage layer to real AWS S3 and compare performance
- Add more sophisticated session logic (e.g., funnel analysis: view → cart → purchase conversion rates)
- Write more thorough integration tests instead of just unit tests per module

## Why this project (some context)

I'm a final-year B.Tech Computer Engineering student, and most of my other projects lean AI/ML (an AI codebase visualizer, a RAG pipeline, an AI accounting assistant). I built this one specifically to demonstrate that I understand data engineering fundamentals — schema design, streaming architecture, data quality handling — not just how to call an LLM API. It's aimed at data engineering and backend-heavy roles where pipeline design actually matters.
