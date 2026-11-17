# Data Engineer Take-Home: Who Is Actually Using the Product?

**Time box: about 3 hours.** Please stop around then. We would rather see a clean,
well-explained partial solution than a rushed complete one. Tell us what you would do next.

## The scenario

We sell a SaaS admin product. Each customer **tenant** (an "org") is managed in the
product; the commercial side lives in a CRM, where one **account** can own several orgs.
The business wants to know which accounts are actually using the product, and on which plan.

You get three extracts in `data/`:

| File | What it is |
|---|---|
| `organizations/YYYY-MM-DD.jsonl` | One file per day (2026-01-01 to 2026-02-28). Each line is a JSON document: the full state of one org that day. Fields: `org_id`, `tenant_name`, `region`, `status`, `license` (`plan`, `seats`, ...), `crm_ref`, `created_at`. |
| `signins.csv` | Sign-in events: `event_id, org_id, user_id, event_ts, result, ip_hash`. Timestamps are UTC. `user_id` and `ip_hash` are already pseudonymised. |
| `crm_accounts.csv` | CRM accounts: `account_id, account_name, country, segment`. |

Treat the files as raw extracts from source systems: do not edit them by hand.

## What to build

1. **A pipeline** (any language and tools: Python, SQL, Spark, DuckDB, dbt, pandas, ...) that
   loads the raw files, cleans them, and builds the model below. Running it twice must give the same result.
2. **A small model**, written out as CSV or Parquet:
   - `dim_account`: one row per CRM account, cleaned.
   - `dim_org`: org history as a **type 2 slowly changing dimension** on `plan`, `seats` and `status`,
     with columns `org_id, plan, seats, status, valid_from, valid_to, is_current`.
     `valid_from`/`valid_to` are dates; `valid_to` is **exclusive** and empty for the current row.
     History starts with the snapshots: an org's first `valid_from` is the first day it appears in
     `organizations/`, not its `created_at`.
   - `fact_signins_daily`: one row per org per day with the number of successful sign-ins,
     failed sign-ins, and distinct users with a successful sign-in.
3. **The answer**: `mau.csv` with columns `account_id, month, plan, mau`.
4. **Data quality checks** of your choice that run as part of the pipeline.
5. **A README**: how to run it, the decisions you made and why, the assumptions you made,
   and what you would do with more time or at 1000x the volume.

## Business rules

- **MAU** (monthly active users) = distinct users with at least one **successful** sign-in in the month.
- **Test tenants** are orgs whose `tenant_name` starts with `Test` or `Demo`. Exclude them from the answer.
- **Plan**: use the plan each org had on the **last day of the month** it appears in the data.
  If one account has orgs on different plans, report one row per plan.
- **Linking orgs to accounts**: CRM account IDs have an 18-character form and a 15-character form;
  the **first 15 characters**, compared ignoring case, identify an account (they are unique even
  ignoring case). Orgs that do not link to any CRM account are excluded from `mau.csv`; tell us how
  many there were (test tenants aside).
- `month` is formatted `YYYY-MM`. `account_id` is the 18-character CRM ID.

Everything else is your call. Where the data surprises you, decide, write down what you decided, and move on.

## Rules of the road

- AI assistants (Copilot, ChatGPT, Claude, ...) are allowed. Say in your README where and how you used them.
  In the follow-up call we will ask you to walk through your code and change something live.
- Send us a zip or a link to a repository with your code, outputs and README.
- Questions about the brief? Email us, but if you are blocked, make an assumption and note it.
