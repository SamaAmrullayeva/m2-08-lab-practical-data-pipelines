# Data Pipeline Architecture — Online Retail

## Task 1: Pipeline Architecture

### 1.1 End-to-End Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                            DATA SOURCES                                          ║
╠══════════════════════════╦═══════════════════════════════════════════════════════╣
║  BATCH SOURCE            ║  STREAM SOURCE                                        ║
║  Online Retail.xlsx      ║  Live Transaction Feed                                ║
║  (historical, one-shot)  ║  (row-by-row, continuous)                             ║
╚══════════════╤═══════════╩══════════════════════╤════════════════════════════════╝
               │                                  │
               ▼                                  ▼
╔══════════════════════════╗       ╔══════════════════════════╗
║  BATCH INGESTION         ║       ║  STREAM INGESTION        ║
║  - Convert xlsx → NDJSON ║       ║  - Accept HTTP POST      ║
║  - Chunk into pages      ║       ║    per transaction       ║
║  - Assign ingestion_ts   ║       ║  - Assign ingestion_ts   ║
║  - Emit per-row events   ║       ║  - Emit to shared queue  ║
╚══════════════╤═══════════╝       ╚══════════════╤═══════════╝
               │                                  │
               └─────────────────┬────────────────┘
                                 │ (unified event stream)
                                 ▼
               ╔══════════════════════════════════╗
               ║       RAW LANDING ZONE           ║
               ║  storage/raw/YYYY/MM/DD/         ║
               ║  Format: NDJSON (append-only)    ║
               ║  Retention: 90 days              ║
               ║  No schema enforcement here      ║
               ╚══════════════╤═══════════════════╝
                              │
                              ▼
               ╔══════════════════════════════════╗
               ║       VALIDATION STAGE           ║
               ║  1. Schema checks                ║
               ║  2. Value range checks           ║
               ║  3. Business rule checks         ║
               ╚════════╤═════════════╤═══════════╝
                        │             │
              (pass)    │             │ (fail)
                        ▼             ▼
               ╔═════════════╗  ╔═════════════════════╗
               ║ CLEAN LAYER ║  ║ QUARANTINE / DLQ    ║
               ║ (Parquet,   ║  ║ storage/quarantine/ ║
               ║  partitioned║  ║ Format: NDJSON +    ║
               ║  by date)   ║  ║ error_reason field  ║
               ╚══════╤══════╝  ║ Alert → ops team    ║
                      │         ╚═════════════════════╝
                      ▼
               ╔══════════════════════════════════╗
               ║     TRANSFORMATION STAGE         ║
               ║  - Cleaning edge cases           ║
               ║  - Derived columns               ║
               ║    (line_total, is_cancellation, ║
               ║     year, month, day_of_week)    ║
               ║  - Customer aggregations         ║
               ║    (RFM features, order counts,  ║
               ║     product diversity)           ║
               ╚══════════════╤═══════════════════╝
                              │
               ╔══════════════╧═══════════════════╗
               ║        FEATURE LAYER             ║
               ║  storage/features/               ║
               ║  - customer_features.parquet     ║
               ║  - daily_sales_agg.parquet       ║
               ║  - product_stats.parquet         ║
               ╚═══════════╤══════════════════════╝
                           │
          ┌────────────────┴───────────────────┐
          │                                    │
          ▼                                    ▼
╔═══════════════════════╗          ╔═══════════════════════╗
║   BI DASHBOARD        ║          ║   ML MODEL            ║
║   Reads daily_sales   ║          ║   Reads               ║
║   + product_stats     ║          ║   customer_features   ║
║   via SQL / Parquet   ║          ║   for training &      ║
║   Updated nightly     ║          ║   daily scoring       ║
╚═══════════════════════╝          ╚═══════════════════════╝

                ╔══════════════════════════════════╗
                ║     MONITORING LAYER             ║
                ║  (runs at every stage boundary)  ║
                ║  - Freshness checks              ║
                ║  - Row count anomaly detection   ║
                ║  - Schema drift detection        ║
                ║  - Null rate tracking            ║
                ║  - DLQ volume alerts             ║
                ╚══════════════════════════════════╝
```

---

### 1.2 Component Descriptions

#### Data Sources

**Batch Source — Online Retail.xlsx**
The historical dataset is a single Excel file containing roughly 541,000 invoice line items from 2010–2011. It is loaded once during the initial pipeline run and thereafter treated as immutable. The ingestion layer reads it sequentially, converts each row to a structured JSON event, and appends it to the raw landing zone. Because the file is processed only once, the ingestion step is idempotent by design: a manifest file records the sha256 hash of the source file, and the pipeline skips the file if a matching hash is already recorded.

**Stream Source — Live Transaction Feed**
New transactions arrive one row at a time via an HTTP POST endpoint that accepts a JSON payload matching the raw transaction schema. This simulates a point-of-sale or order management system emitting events in real time. The ingestion endpoint is stateless: it validates the presence of required envelope fields (not business logic), stamps the event with `ingestion_ts`, and writes it to the same raw landing zone as the batch path. Both paths converge here, eliminating the need for two separate downstream processing chains.

---

#### Ingestion Layer

**Batch Ingestion**
The batch ingestion component opens the xlsx file using a streaming reader (to avoid loading 541K rows into memory at once), converts each row to a newline-delimited JSON record, and appends it to the raw landing zone partitioned by today's date. It records a high-water mark of the last-processed row offset so that a partial run can resume rather than reprocess the full file. Input is the xlsx file path; output is NDJSON files under `storage/raw/YYYY/MM/DD/batch/`.

**Stream Ingestion**
The stream ingestion component exposes a thin HTTP endpoint. It performs only structural envelope checks (is the payload valid JSON? are required envelope keys present?), stamps the record with an `ingestion_ts` and a UUID `event_id`, then writes the record to `storage/raw/YYYY/MM/DD/stream/`. It responds 202 Accepted immediately and never applies business validation — that belongs to the validation stage. This keeps the ingestion path fast and decoupled from validation logic.

---

#### Raw Landing Zone

Raw storage is an append-only NDJSON store partitioned by `ingestion_date`. NDJSON is chosen here rather than Parquet because the raw layer must accept records before their schema has been validated — a strict columnar format would reject structurally malformed rows. Every raw file is immutable once written; nothing downstream modifies it. This design allows full pipeline replay: if validation logic changes, the clean and feature layers can be rebuilt from raw without re-ingesting from source. Retention is 90 days, after which raw files are archived to cold storage.

---

#### Validation Stage

The validation stage reads raw NDJSON records, applies three tiers of checks (schema, value range, business rules — detailed in Task 2), and routes each record to exactly one of two outputs: the clean layer (pass) or the quarantine store (fail). It is implemented as a stateless function so it can be run in parallel across date-partitioned raw files. Every record that passes validation is enriched with a `validated_at` timestamp and a `source` tag (`batch` or `stream`) before being written to the clean layer.

---

#### Quarantine / Dead Letter Queue

Failed records are written to `storage/quarantine/YYYY/MM/DD/` as NDJSON, with the original record preserved verbatim alongside a structured `error` object containing: the failing rule name, the offending field, the observed value, and the expected constraint. Quarantine files are never deleted automatically — they require operator action. When the volume of quarantined records in a single partition exceeds a configurable threshold (default: 1% of total records for that partition), an alert is sent to the operations channel. Recovery is described in section 2.2.

---

#### Clean Layer

The clean layer stores validated records as Parquet files partitioned by `InvoiceDate` (year/month/day). Parquet is chosen because downstream consumers — both the transformation stage and ad-hoc analysts — issue column-selective queries (e.g., `SELECT CustomerID, UnitPrice, Quantity WHERE Country = 'UK'`). Parquet's columnar compression cuts storage cost by roughly 5–8x compared to NDJSON at this row count and makes those queries significantly faster. Each partition is written once and treated as immutable; re-processing overwrites the partition atomically using a write-to-temp-then-rename pattern.

---

#### Transformation Stage

The transformation stage reads from the clean layer, applies all derived-column and aggregation logic (detailed in Task 3), and writes results to the feature layer. It is split into two sub-jobs: a daily incremental job that processes new clean partitions and refreshes affected customer aggregations, and a weekly full-recompute job that rebuilds all customer features from the full history window (necessary for the ML model's training run). Each transformation is written as a pure function of its inputs so it can be re-run safely.

---

#### Feature Layer

The feature layer contains three Parquet tables: `daily_sales_agg` (date × country × product summaries for the BI dashboard), `product_stats` (per-StockCode metrics), and `customer_features` (one row per CustomerID with RFM and ML features). These are the only tables that downstream consumers read — they never query the raw or clean layers directly. This isolation means the pipeline can change its internal representation without breaking consumers.

---

#### Consumer Interfaces

**BI Dashboard**: Reads `daily_sales_agg` and `product_stats` via a SQL query engine (e.g., DuckDB or a thin Parquet-over-S3 layer). Updated nightly by a scheduled run of the transformation stage. The dashboard team is given read-only access to the feature layer path and no access to raw or clean layers.

**ML Model**: Reads `customer_features` as a Pandas DataFrame for weekly retraining and daily feature refresh for online scoring. The feature table is built with a strict observation-window cutoff to prevent data leakage: features are computed using only transactions with `InvoiceDate < cutoff_date`, where `cutoff_date` is parameterised at run time.

---

#### Monitoring Layer

The monitoring layer runs automated checks at three points in the pipeline: after raw ingestion, after validation, and after transformation. It tracks: record counts per partition (alerting on >20% drop vs. rolling 7-day average), null rates per field (alerting if any required field exceeds 0.1% nulls), schema consistency (comparing the set of observed column names against a registered schema version), DLQ volume (absolute count and percentage), and feature freshness (maximum age of the newest record in `daily_sales_agg`). Alerts are emitted as structured log events and optionally forwarded to a notification channel. All metrics are written to a `monitoring/metrics/` Parquet table for historical trending.

---

## Task 2: Validation and Error Handling Design

### 2.1 Validation Rules

#### Category A — Schema Validations (structural correctness)

| # | Field | Expected Type | Required | Rule |
|---|---|---|---|---|
| S1 | InvoiceNo | string | yes | Non-empty; matches pattern `^C?\d{6}$` |
| S2 | StockCode | string | yes | Non-empty; length 1–20 characters |
| S3 | Description | string | no | If present, must be a non-empty string (not whitespace-only) |
| S4 | Quantity | integer | yes | Parseable as a whole number (no decimals) |
| S5 | InvoiceDate | datetime string | yes | Parseable as ISO 8601 or `MM/DD/YYYY HH:MM` format |
| S6 | UnitPrice | float | yes | Parseable as a non-null floating-point number |
| S7 | CustomerID | string or integer | no | If present, must be a 5-digit numeric value |
| S8 | Country | string | yes | Non-empty; length ≥ 2 characters |

**S1 detail**: The pattern `^C?\d{6}$` captures normal invoices (`536365`) and cancellations (`C536379`). Any InvoiceNo outside this pattern (e.g., containing letters other than a leading C, or wrong digit count) is a schema violation.

#### Category B — Value Range Validations (sensible values)

| # | Field | Rule |
|---|---|---|
| V1 | UnitPrice | Must be ≥ 0; negative prices are not permitted even for cancellations |
| V2 | UnitPrice | For non-cancellation records (InvoiceNo does not start with `C`): must be > 0 |
| V3 | Quantity | For non-cancellation records: must be > 0 |
| V4 | Quantity | Absolute value must be ≤ 80,000 (extreme outlier threshold derived from dataset analysis) |
| V5 | InvoiceDate | Must fall between 2009-01-01 and `now() + 1 day` (reject clearly impossible future dates) |
| V6 | UnitPrice | Must be ≤ 25,000 (extreme outlier threshold; legitimate max in dataset ~13,541) |

#### Category C — Business Rule Validations (domain logic)

| # | Rule | Description |
|---|---|---|
| B1 | Cancellation sign agreement | If `InvoiceNo` starts with `C`, then `Quantity` must be negative |
| B2 | Cancellation sign agreement (reverse) | If `Quantity` is negative, then `InvoiceNo` must start with `C` |
| B3 | Zero-price non-cancellation | If `UnitPrice == 0` and `InvoiceNo` does not start with `C`, flag as anomaly (possible free sample or data error) — quarantine for review rather than hard reject |
| B4 | Description–StockCode consistency | Records sharing the same `StockCode` must not have more than 10 distinct `Description` values across the entire dataset (drift check run in batch, not per-record) |
| B5 | Customer invoice consistency | All line items sharing the same `InvoiceNo` must have the same `CustomerID` (a single invoice cannot belong to two customers) |

---

### 2.2 Error Handling Flow

#### Schema Violations (S1–S8)

**Disposition**: Hard reject. A record that cannot be parsed into the expected structure cannot be safely reasoned about by downstream logic. It is not partially accepted.

**Destination**: Written to `storage/quarantine/YYYY/MM/DD/schema_errors/` as NDJSON. The quarantine record contains the original raw payload (as a string field `raw`) plus an `error` object: `{ "category": "schema", "rule": "S1", "field": "InvoiceNo", "observed": "XYZ99", "expected": "pattern ^C?\\d{6}$" }`.

**Alert**: If schema errors exceed 0.5% of the partition's total records, an alert fires immediately. A sudden spike in schema errors indicates a source system format change.

**Recovery**: An operator inspects quarantine records, identifies the root cause (e.g., upstream system changed the InvoiceNo format), updates the schema rule or source transformation accordingly, then re-runs the validation stage against the affected raw partition. The quarantine files are not deleted until recovery is confirmed.

---

#### Value Range Violations (V1–V6)

**Disposition**: Hard reject for V1–V5. Rule V6 (extreme outlier UnitPrice) triggers a soft quarantine: the record is quarantined but tagged `review_required` rather than `rejected`, because extremely high unit prices may be legitimate (e.g., large machinery, custom orders).

**Destination**: `storage/quarantine/YYYY/MM/DD/range_errors/`. Same structure as schema errors with appropriate `category: "value_range"`.

**Alert**: Threshold is 1% of partition volume for hard rejects. Soft-quarantined outlier records are surfaced in a daily data quality report rather than triggering an immediate alert, since individual high-value records are expected occasionally.

**Recovery**: For systematic range errors (e.g., a batch file where UnitPrice was accidentally stored in pence rather than pounds), the operator corrects the source file or writes a one-time transformation to rescale values, then replays the affected raw partition through the validation stage.

---

#### Business Rule Violations (B1–B5)

**Disposition**: B1 and B2 are hard rejects — sign disagreement between InvoiceNo and Quantity is unambiguous data corruption. B3 is a soft quarantine (review required). B4 is logged as a batch-level anomaly flag, not a per-record reject (StockCode description drift doesn't invalidate individual records). B5 is a hard reject for the conflicting line items — when two line items on the same InvoiceNo disagree on CustomerID, all items for that InvoiceNo are quarantined together.

**Destination**: `storage/quarantine/YYYY/MM/DD/business_rule_errors/`.

**Alert**: Any B1/B2 violations trigger an immediate alert because they indicate either a system bug or deliberate data manipulation. B5 (CustomerID conflict on same invoice) is also immediately alerted. B3 and B4 are included in the daily quality report.

**Recovery**: Business rule violations often require human judgment about which version of the truth is correct. The recovery flow requires operator sign-off: the operator either corrects the source record and re-ingests, or marks the quarantined record as `manually_resolved` with a justification note, after which it can be re-introduced into the clean layer with a `manually_corrected: true` flag.

---

## Task 3: Transformation and Storage Design

### 3.1 Transformation Definitions

#### T1 — Whitespace and Encoding Cleanup

**Input**: Clean-layer records (post-validation)  
**Output**: Records with normalised string fields  
**Transformations**: Strip leading/trailing whitespace from `Description`, `StockCode`, `Country`; normalise `Description` to uppercase for consistency with the majority of the dataset's existing values; replace empty-string `Description` with `null`.  
**Idempotency**: Yes — applying these string operations twice produces the same result as applying them once.

---

#### T2 — Derived Column: line_total

**Input**: `Quantity` (integer), `UnitPrice` (float)  
**Output**: New column `line_total = Quantity * UnitPrice` (float, rounded to 2 decimal places)  
**Idempotency**: Yes — deterministic arithmetic.

---

#### T3 — Derived Column: is_cancellation

**Input**: `InvoiceNo` (string)  
**Output**: New boolean column `is_cancellation = InvoiceNo.startswith('C')`  
**Idempotency**: Yes.

---

#### T4 — Derived Date Components

**Input**: `InvoiceDate` (datetime)  
**Output**: New columns `invoice_year` (int), `invoice_month` (int), `invoice_day` (int), `day_of_week` (int, 0=Monday), `hour` (int)  
**Idempotency**: Yes — deterministic date arithmetic.

---

#### T5 — CustomerID Normalisation

**Input**: `CustomerID` (may be int or string from source)  
**Output**: `customer_id` (string, zero-padded to 5 digits, e.g., `"12345"`; `null` for guest transactions where CustomerID was absent)  
**Idempotency**: Yes.

---

#### T6 — Customer-Level Revenue Aggregation

**Input**: All clean-layer records for a given `customer_id` within the observation window  
**Output**: One row per customer with: `total_revenue` (sum of `line_total` excluding cancellations), `total_cancellation_value` (sum of absolute `line_total` for cancellations), `net_revenue` (total_revenue − total_cancellation_value)  
**Idempotency**: Yes, given a fixed observation window. The observation window end date is always passed as an explicit parameter — never derived from `now()` — to guarantee reproducibility.

---

#### T7 — Customer-Level Order and Recency Features

**Input**: Clean-layer records, observation window  
**Output**: Per customer: `order_count` (count of distinct non-cancellation InvoiceNos), `first_order_date`, `last_order_date`, `recency_days` (days since `last_order_date` as of `observation_end`), `avg_order_value` (net_revenue / order_count)  
**Idempotency**: Yes, given fixed `observation_end`.

---

#### T8 — Customer-Level Product Diversity Feature

**Input**: Clean-layer records, observation window  
**Output**: Per customer: `unique_products` (count of distinct StockCodes purchased, excluding cancellations), `unique_countries` (count of distinct Countries on invoices — relevant for B2B customers)  
**Idempotency**: Yes.

---

#### T9 — Daily Sales Aggregation (BI Layer)

**Input**: Clean-layer records for a given date partition  
**Output**: Rows of shape `(invoice_date, country, stock_code, description, total_quantity, total_revenue, order_count)`, where `total_quantity` and `total_revenue` exclude cancellations, and cancelled quantities are tracked separately in `cancelled_quantity`  
**Idempotency**: Yes — aggregate over a fixed date partition. Re-running overwrites the existing output partition atomically.

---

#### T10 — Anomaly Flag: High Quantity

**Input**: Clean-layer records  
**Output**: Boolean column `quantity_anomaly = abs(Quantity) > 2 * stddev(Quantity)` computed over a rolling 30-day window per StockCode  
**Idempotency**: Not fully idempotent — the rolling window depends on historical data. Made idempotent by materialising the per-StockCode rolling statistics as a separate reference table updated daily; the flag is then a deterministic lookup against that table.

---

### 3.2 Storage Layers

| Layer | Contents | Format | Update frequency | Retention |
|---|---|---|---|---|
| Raw | Original records exactly as received, no transformation | NDJSON, partitioned by `ingestion_date` | Append-only; continuous | 90 days active, then cold archive |
| Clean | Validated records with normalised types; T1–T5 applied | Parquet, partitioned by `invoice_year/invoice_month/invoice_day` | Nightly batch + near-real-time stream flush (hourly) | Indefinite |
| Feature | Customer aggregations, daily sales summaries, product stats | Parquet (columnar), unpartitioned or partitioned by `as_of_date` for time-travel | Daily (customer_features, daily_sales_agg); weekly full recompute | 2 years of snapshots; older snapshots archived |

#### Format Justifications

**Raw — NDJSON**: Records arrive before schema validation; a strict columnar format cannot accept structurally malformed rows. NDJSON is human-readable, trivially appendable, and requires no schema declaration at write time. Compression (gzip per-file) keeps storage cost manageable. Query performance doesn't matter here — raw is write-optimised and replay-optimised, not query-optimised.

**Clean — Parquet**: The clean layer is the primary query layer for the transformation stage and for ad-hoc analyst access. Parquet's columnar layout is ideal for the query patterns here: analysts typically select 3–5 columns out of 8, and aggregations over `UnitPrice` × `Quantity` benefit from column-level min/max statistics for predicate pushdown. Date-partitioned Parquet also makes incremental processing straightforward: the transformation stage simply reads the new date partitions since its last run.

**Feature — Parquet**: Feature tables are small (one row per customer ≈ ~4,400 rows; daily sales ≈ ~1,000 rows/day) and read in bulk by the ML training job and the BI dashboard. Parquet is sufficient; no need for a database engine. Snapshots are retained by `as_of_date` to support time-travel: this lets the ML team re-train on historical feature sets for model validation without rebuilding the pipeline.

---

### 3.3 Incremental Update Strategy

#### Tracking Processed Data

The pipeline maintains a **high-water mark table** at `storage/metadata/watermarks.json`. Each pipeline component records: `{ "component": "validation", "last_processed_partition": "2011/12/09", "last_run_ts": "2025-06-02T03:00:00Z" }`. On each run, the component reads its watermark, processes all raw partitions newer than `last_processed_partition`, and updates the watermark atomically after successful completion. This prevents double-processing and provides a clear recovery point after failures.

For the stream path, ingestion is continuous; the validation stage flushes and processes the current hour's stream files on an hourly schedule rather than waiting for the nightly batch run.

#### Handling Late-Arriving Records

Late-arriving records — transactions with an `InvoiceDate` in the past that arrive after that date's clean partition has already been processed — are a known pattern (e.g., delayed sync from an offline terminal). The pipeline handles them with a **grace period** of 7 days: any record with an `InvoiceDate` within the last 7 days causes the pipeline to re-process the affected clean partition, overwriting it atomically. Records with `InvoiceDate` older than 7 days and arriving late are quarantined as `late_arrival` and require operator action to determine whether backfilling is appropriate.

Customer-level aggregation features (T6–T8) are always computed from the full clean-layer history (within the observation window), not incrementally, so late-arriving records within the grace period are automatically captured in the next daily feature refresh — no special handling needed at the feature layer.

#### Feature Refresh Cadence

| Feature table | Trigger | Method |
|---|---|---|
| `daily_sales_agg` | Nightly, after clean layer is updated | Incremental: compute only new date partitions; append to existing table |
| `customer_features` | Nightly (for BI dashboard) and weekly (full recompute for ML training) | Nightly: recompute only customers with new activity in the last 24h. Weekly: full recompute of all customers with explicit `observation_end = training_cutoff_date` |
| `product_stats` | Nightly | Incremental: update rolling statistics using new clean-layer partitions; materialise updated statistics reference table used by T10 |

The weekly full recompute is stored as a **versioned snapshot** under `storage/features/customer_features/as_of_date=YYYY-MM-DD/` so the ML team can always reconstruct the exact feature set used for any historical model training run. This also enables offline model validation: training on week N's features and evaluating on week N+1's labels.
