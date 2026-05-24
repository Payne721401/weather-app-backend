# Weather Backend

Backend service for the Weather app. Fetches data from Taiwan's Central
Weather Administration (CWA), the National Science and Technology Center
for Disaster Reduction (NCDR), and the Ministry of Environment (MONEV).
Persists to Firestore and Cloudflare R2, and reports status via Telegram.

## Overview

The service is a collection of scheduled tasks driven by GitHub Actions.
Scheduling is delegated to an external Google Apps Script that fires a
`repository_dispatch` event every 10 minutes; the
[api-triggered-scheduler.yml](.github/workflows/api-triggered-scheduler.yml)
workflow decides which tasks to run based on the current Taipei time.

```
Apps Script (every 10 min)
        │  repository_dispatch: run-weather-tasks
        ▼
api-triggered-scheduler.yml
        │  hour / minute -> task list
        ▼
functions/main.py -> functions/weather/*  -> Firestore / R2
                  \\-> functions/services/notification.py -> Telegram
```

## Schedule

All times in Taipei time (UTC+8).

| Task | Source | Destination | Frequency |
|---|---|---|---|
| `update_radar` | CWA F-B0046-001 | R2 (gzipped JSON) | every 10 min |
| `update_uv_index` | CWA O-A0005-001 | Firestore `uv_index` | hourly, :05-:14 |
| `update_air_quality` | MONEV aqx_p_432 | Firestore `air_quality` | hourly, :05-:14 |
| `update_current_weather` | CWA O-A0001-001 | Firestore `observations` | every 2 hours |
| `update_three_hour_forecast` | CWA F-D0047-093 | Firestore `weather_forecasts` | every 3 hours |
| `update_weekly_forecast` | CWA F-D0047-093 | Firestore `weather_forecasts` | 06:05, 18:05 |
| `update_sunrise_sunset` | CWA A-B0062-001, A-B0063-001 | Firestore `sunrise_sunset` | 00:05 daily |
| `update_typhoon_forecast` | CWA SMCA image | R2 (PNG) | 03:05, 15:05 |

See [api-triggered-scheduler.yml](.github/workflows/api-triggered-scheduler.yml)
for the full dispatch logic.

## Project layout

```
functions/
├── main.py                 # CLI entry point, dispatches to update_* tasks
├── config/settings.py      # env-var-backed configuration
├── services/
│   ├── weather_api.py      # HTTP client for CWA / NCDR / MONEV
│   └── notification.py     # Telegram client
├── weather/                # one module per task (current_weather, forecast, ...)
├── database/models.py      # Firestore models, R2 client, batch save
└── utils/data_processing.py
tests/
├── unit/                   # no network, runs in CI
└── integration/            # hits real APIs, manual only
.github/workflows/
├── api-triggered-scheduler.yml  # production scheduler
└── run-integration-tests.yml    # CI (unit + lint)
```

## Local development

Requires Python 3.12+.

```bash
git clone <repo>
cd Weather_Backend
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements_dev.txt
```

Create a `.env` file at the repository root with the variables listed
below. Then run any single task:

```bash
python functions/main.py update_radar
python functions/main.py update_current_weather
```

The full list of task names is in
[functions/main.py](functions/main.py) (`argparse` choices).

## Environment variables

Required for both local development (via `.env`) and GitHub Actions
(via repository Secrets):

| Name | Purpose |
|---|---|
| `CWA_API_KEY` | Central Weather Administration API |
| `NCDR_API_KEY` | National Science and Technology Center for Disaster Reduction |
| `MONEV_API_KEY` | Ministry of Environment air quality API |
| `GCP_SA_KEY_BASE64` | Firebase service account JSON, base64-encoded |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 access key |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 secret |
| `TELEGRAM_BOT_TOKEN` | Telegram bot for status notifications |
| `TELEGRAM_CHAT_ID` | Telegram chat to post into |

Two additional values are stored as GitHub Actions Variables (not
Secrets) because they are not sensitive: `R2_BUCKET_NAME`,
`R2_ENDPOINT_URL`.

## Testing

```bash
pytest -m unit            # fast, no network, no secrets needed
pytest -m integration     # hits real CWA / NCDR / MONEV APIs, needs .env
```

Integration tests are automatically skipped when the required API keys
are not present in the environment, so a clean `pytest` run on a machine
without `.env` will not fail.

CI runs only the unit tests on every push and pull request. Integration
tests are gated behind manual `workflow_dispatch` with the
`run_integration` input set to `true`.

## Manual operations

All from the GitHub Actions tab:

- **Run all due tasks for the current time window**: trigger
  `Unified Smart Scheduler` with no input.
- **Run a single task**: trigger `Unified Smart Scheduler` and set the
  `task` input to one of the task names (e.g. `update_radar`).
- **Run integration tests**: trigger `CI` and check `run_integration`.

## Monitoring

Telegram is the source of truth for task status. Each task posts a
success or failure message on completion. Common failure modes:

- **CWA 429 / quota exceeded** — wait for the next scheduled run.
- **Firestore `ResourceExhausted`** — the daily free-tier quota is
  exhausted; `batch_save` short-circuits the remaining writes and the
  task raises.
- **R2 upload error** — verify `R2_ACCESS_KEY_ID` and
  `R2_ENDPOINT_URL`.
