# Data Engineering Core View Assigment

## 1. Summary

This project loads the three raw extracts (organisation snapshots, sign-in
events and CRM accounts), cleans them, and builds a small dimensional model
plus the requested answer, `mau.csv`: monthly active users per CRM account,
per month, per plan.

It is a Databricks pipeline written in PySpark and Spark SQL, organised in
three layers (landing → bronze → silver → gold) and run as a single
Databricks job. It is idempotent: [how – e.g. every table is fully
overwritten from the raw files on each run].

Outputs:

- `dim_account`, `dim_org` (SCD2 on plan, seats and status), `fact_signins_daily`
- `mau.csv` – 361 rows (181 for 2026-01, 180 for 2026-02)

## 2. How to run it

Prerequisites: a Databricks workspace ([Free Edition / which one you used]) with
Unity Catalog and Jobs available.

1. Clone this repo into your workspace (Workspace → Create → Git folder). Name the cloned repository 'coreview'.
2. Upload the source files to the catalog 'workspace' by creating a volume called 'organizations_data'. To the following path :
   '/Volumes/workspace/default/organizations_data'.
   The source files are in the `data/` zip in the repo; unzip it first.
3. Open `jobs.yaml` and replace the placeholder user email with your databricks account email.
4. Go to Jobs & Pipelines → Create job, click the ⋮ menu → Edit as YAML,
   and replace the content present by pasting the contents of `jobs.yaml`.
5. click save and Run the job.

Outputs are written to [coreview.gold] schema.

Repository layout:

    data/                  zipped raw extracts
    notebooks/
      landing_to_bronze   [exact notebook name]
      bronze_to_silver
      silver_to_gold
    outputs/
      dim_accounts.csv
      dim_org.csv
      fact_signins.csv
      mau.csv
    jobs.yaml              job definition (3 tasks, run in order)
    README.md

Re-running: Every table in each layer is overwritten. so data is not appended. In an ideal scenario this would be an incremental pipeline.

## 3. Pipeline overview

| Layer            | Notebook          | What it does                                                                                | Tables / files                                                                |
| ---------------- | ----------------- | ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Landing → Bronze | landing_to_bronze | Reads the raw files as-is, all columns as strings, adds the Ingestion time                  | crm_accounts, organizations, signins                                          |
| Bronze → Silver  | bronze_to_silver  | Cleans, deduplicates and standardises; builds clean tables, rejects corrupted records       | crm_accounts, organizations, signins, signins_quarantine                      |
| Silver → Gold    | silver_to_gold    | Builds `dim_accounts`, `dim_organizations` , `fact_signins` and `mau` and writes the output | dim_accounts, dim_organizations, fact_signins, staging_org_attributes, vw_mau |

Data flow: `organizations/*.jsonl`, `signins.csv` and `crm_accounts.csv` feed
the bronze tables, then the silver tables, then the gold tables and `mau.csv`.

## Decisions, Assumptions and why

### Landing and Bronze

**Keep a landing layer.**
Raw files are stored exactly as received, in a `yyyy/mm/dd` folder structure (Not used in this Assignment)
that follows the date of each run. This keeps an untouched copy of the source
for audit and replay, and makes it easy to see which file arrived on which
run date.

**Record the source file name for every row.**
Every bronze row stores the file it came from. This gives lineage (any row
can be traced back to its file) and supports incremental loads and
partitioning. For the organisation files it is more than metadata: the
file name is the snapshot date, which is the date the org state was
captured and is needed to build the history.

### Bronze to Silver

**General cleaning and quality handling.**
Beyond the decisions below, the silver layer applies routine cleaning so
downstream tables can rely on consistent data:

- Types are set explicitly (for example, `seats` is cast to a number).
- Text is trimmed and standardised (plan, status, CRM references).
- Sign-in timestamps arrive in two formats; both are converted to a UTC
  timestamp.
- 8,751 duplicate sign-in events are removed. Once the timestamps are
  normalised, they are exact copies.
- Helper columns are derived: the 15-character CRM join key, the test-tenant
  flag (names starting with `Test` or `Demo`), and event dates and months.
- Records that cannot be trusted are quarantined, not dropped (see below).

**Standardise country to codes.**
The `country` column in the CRM extract has the same country written many
ways (for example France / france / FR, or several spellings of the United
Kingdom), sometimes with extra spaces. I trimmed the values and mapped them
all to one two-letter country code, so each country has exactly one value.
If the reporting team prefers full names, the only change needed is the
mapping table, or they can keep a code-to-name lookup.

**Carry forward the last known plan and seats when the licence is missing.**
Some rows (13 in this case) have no licence data, so plan and seats are null. A live org
cannot really have no plan, so I treat these as corrupt records, not
real changes. For each of them I reuse the last known plan and seats for
that org.

- This is an assumption. I had no way to ask the source team why it
  happens or how they normally fix it. The plan may have genuinely
  changed on that day, but we can't know, so the last known value is the
  safest choice.
- It matters because the MAU rule uses the plan on the last day of the
  month, and there are gaps falling exactly on those days (01-31 and
  02-28). Without the fill, those orgs would have no plan in the answer.

**Quarantine sign-ins from orgs that are not in the org data.**
500 sign-in rows belong to org IDs that do not exist in any org snapshot.
I did not delete them and did not pass them on. They are held in a
quarantine table. They might be genuine rows from orgs our org extract has
not caught up with, so deleting them would lose data. But passing them
downstream could distort the aggregations and produce wrong reports.
Quarantining keeps them safe, counted and ready to reprocess once the
missing orgs arrive.

### Silver to Gold

**`dim_account` is SCD Type 1 (current values only).**
The brief asks for one cleaned row per account, and the CRM extract is a
single snapshot with no history, so there is nothing to track yet.
In a real setting I would use SCD Type 2 here too, because account name and
segment can change, even if rarely, and the business may want to report on
the history. Moving to Type 2 would only need a change date from the
source, or the load date, as `valid_from`.

**`dim_org` is built as a Type 2 history from the daily snapshots.**
I order each org's snapshots by date and start a new version whenever
`plan`, `seats` or `status` changes. A version's `valid_from` is the date of
its first snapshot. Its `valid_to` is the start date of the next version
(exclusive), and it is empty on the current row.

**Descriptive attributes are in a separate table.**
Fields that never change (name, region, CRM reference, created date) are
kept in their own org table, linked by `org_id`. This follows normal
dimensional modelling: it avoids repeating the same text on every history
row and keeps `dim_org` small and focused on what changes.

**MAU is built from the cleaned sign-ins, not from `fact_signins_daily`.**
The fact table is already aggregated per org per day, and distinct users
cannot be added up across days or orgs without double counting. MAU needs
distinct users over a whole month per account and plan, so it is calculated
from the cleaned row-level data.
Only successful sign-ins count. A failed attempt is not an actual login,
so it does not show that a user is active.

## Technical and Business rules applied

**Time.**
All sign-in timestamps are UTC. Timestamps that arrive without a zone marker
are also treated as UTC, as the brief states. The pipeline relies on the Spark
session time zone being UTC. "Month" and "day" always mean the UTC calendar
month and day.

**Duplicates and event IDs.**
`event_id` identifies a sign-in event. A row is a duplicate only when it has
the same `event_id` and the same content. Two rows with different `event_id`s
are kept as separate events, even if the org, user, time, result and IP match.
This happens for 3 pairs of rows. Counts for those orgs may be slightly
higher, but MAU is not affected because it counts distinct users.

**Missing licence data.**
A null licence is treated as a corrupt record, not as a real change: the
last known plan and seats are carried forward (see Decisions).

**Sign-ins for unknown orgs.**
The 500 sign-ins whose `org_id` is not in the org data may be genuine. They
are quarantined, not deleted, so they can be reprocessed if those orgs appear
later. They are excluded from every downstream table and from MAU.

**Sparse daily fact.**
`fact_signins` has a row only when an org had at least one sign-in
that day. Days with no sign-ins are not filled in with zeros, because of the
time limit for the assignment. Anyone who needs a complete calendar can build it from a date
table.

**Test tenants.**
Orgs whose name starts with `Test` or `Demo` stay in `dim_org` and the fact
table, because those hold the full history of what exists in the product.
They are excluded only when MAU is built.

**Org history.**
An org's history starts on the first day it appears in the snapshots, not on
its `created_at`. An org that was cancelled stays in its last snapshot with
status `cancelled`, so its last version is still the current one. In this
data no sign-in happens after a cancellation.

**Plan in MAU.**
The plan for each month is the plan on the last snapshot day of that month
(2026-01-31 and 2026-02-28). If an account has orgs on different plans in a
month, it gets one row per plan.

**Accounts without MAU.**
5 of the 180 CRM accounts do not appear in `mau.csv`. They have no linked org
or no successful sign-ins in the period.

## With more time, and at 1000x the volume

### With more time

**Testing and data quality**

- Unit tests for the transformations (country mapping, null-licence fill,
  SCD2 versioning, timestamp parsing), plus a small test dataset that runs
  end to end.
- Move the quality checks into a reusable framework with configurable rules,
  severity (warn or fail) and a results table that can be trended over time.
- Add a complete calendar for `fact_signins` so days with no sign-ins
  show as zero.

**Incremental processing**

- Replace the full reload with a daily incremental load. A watermark (the last
  processed file date or load timestamp) means each run handles only new data.
- Remove duplicates at the raw level using unique keys (`event_id` for
  sign-ins, `org_id` + snapshot date for orgs), with `MERGE` so a re-run or a
  late file never creates duplicates.
- Build `dim_org` incrementally: compare each new snapshot with the current
  version and only close and open versions when `plan`, `seats` or `status`
  change.
- Reprocess quarantined sign-ins automatically when the missing orgs arrive.

**Orchestration**

- Run on a schedule or on file arrival, instead of a manual start.
- Use one parameterised pipeline that runs the notebooks dynamically
  (source name, run date, layer), instead of a separate set of notebooks per
  source.

**Reusable ingestion frameworks**

- Build metadata-driven frameworks for bronze and silver, so adding a new
  source means adding configuration (schema, keys, cleaning rules), not new
  code.

**Monitoring and alerting**

- Alerts at each major stage (ingestion, quality checks, row-count
  reconciliation, job failure and long run times) sent by email or chat.
- Track row counts, rejects and quarantine volumes per run.

**CI/CD**

- Version control with pull-request review, automated tests on every
  change, and deployment of notebooks and job definitions across dev, test and
  prod environments with infrastructure as code (for example Databricks Asset
  Bundles).

### At 1000x the volume

- **Storage and layout:** keep data in Delta tables partitioned by date
  (and clustered on `org_id` where it helps). Compact small files regularly,
  so reads stay fast.
- **Compute:** move from a single small cluster to autoscaling clusters
  sized for the job, with Photon where available. Tune shuffles and joins, and
  broadcast the small dimensions (accounts, orgs) so the large sign-in table is
  not shuffled.
- **Deduplication:** a full-table `distinct` over all sign-ins does not scale.
  implement a better scalable operation.
- **MAU:** exact distinct counts get expensive at this scale. implement a better scalable operation.
- **Streaming:** if sign-ins arrive continuously, ingest them with
  Auto Loader or structured streaming instead of daily files.
- **Cost:** use job clusters instead of always-on ones, prune partitions, and
  avoid reprocessing history. Only recompute the current and previous months,
  since older months do not change.
- **Governance:** access controls and column-level protection on user and IP
  identifiers, even though they are already pseudonymised.

## Use of AI

I used AI tools while building this project. Below is where and how, and
what I did myself.

**Claude**

- To understand the brief and its business rules.
- To understand the data: what problems to expect (duplicates, inconsistent
  formats, missing values, unmatched keys) and ways to handle them.
- To write and refine this documentation.

**Databricks Genie**

- To write parts of the notebook code.
- To help find and fix bugs.
- To format and comment the notebooks.

**Other AI tools**

- For complex code that Genie produced, I used other AI tools to explain how it
  works. I then wrote my own version of the logic where I wanted to.

**My own work**
Every decision in this README is mine: what to clean and how, the country
codes, carrying the last plan forward, quarantining unknown orgs, how `dim_org`
is versioned, and how MAU is calculated. AI suggested options and wrote code,
and I chose between them.

I read every line of code and documentation that AI produced, and I made sure I understood what it does and why before keeping it. I can walk through any
part of the pipeline and change it live.

I also compared the final `mau.csv` against an independent recalculation from
the raw files, and the two matched exactly.
