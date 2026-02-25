PaketVPN Telegram Bot (aiogram 3)
=================================

Async Telegram bot built on aiogram 3 + aiosqlite with payments, referrals, gift subscriptions, background sync jobs, and healthcheck endpoint.

Quick Start
-----------
1. Install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```
2. Configure environment:
- Create `project/.env` with bot/payment/panel credentials.
- Important for Docker: `DB_PATH=/data/bot.db`
3. Run locally:
```bash
python -m project.app.main
```

Docker Run
----------
From `project/`:
```bash
docker compose up -d --build
```

CI/CD (One Push -> Build -> Deploy)
-----------------------------------
Pipeline file: `.github/workflows/build-and-deploy.yml`

Flow:
1. Push to `main`.
2. GitHub Actions builds Docker image from `project/Dockerfile`.
3. Image is pushed to GHCR (`ghcr.io/<owner>/<repo>/paketvpn-bot:latest`).
4. Workflow connects to your server by SSH.
5. Workflow uploads latest `project/docker-compose.yml` to `DEPLOY_PATH`.
6. If git repo exists on server, workflow pulls deploy branch there (`ff-only`).
7. Server creates DB backup (`sqlite3 .backup` if available, otherwise file copy).
8. Server pulls latest image and restarts `paketvpn-bot` container with healthcheck validation.

Required GitHub Secrets
-----------------------
- `DEPLOY_HOST` - server IP or domain.
- `DEPLOY_PORT` - SSH port (optional, default `22`).
- `DEPLOY_USER` - SSH user.
- `DEPLOY_SSH_KEY` - private SSH key for deploy user.
- `DEPLOY_PATH` - absolute path on server where `docker-compose.yml` is located (usually `.../project`).
- `DEPLOY_DB_DIR` - optional DB directory on server (default: `..`, so `.../project/..` -> `.../bot.db`).
- `DEPLOY_ENV_FILE` - optional env file path on server (default: `../.env`, so `.../project/../.env`).
- `DEPLOY_BRANCH` - optional server git branch for source sync (default: `main`).

One-Time Server Setup
---------------------
1. Install Docker + Docker Compose plugin.
2. Place repo on server (or at least `project/docker-compose.yml`).
3. Ensure external network exists:
```bash
docker network create remnawave-network || true
```
4. Put production env file at `~/Bot/.env` (or set custom `DEPLOY_ENV_FILE`).
5. First manual boot:
```bash
cd project
export BOT_ENV_FILE=../.env
docker compose up -d
```

DB Persistence
--------------
- `docker-compose.yml` binds host directory to container `/data` via `DB_HOST_DIR` (default `..`).
- Effective DB path in container: `/data/bot.db`.
- With layout:
  - `~/Bot/project/docker-compose.yml`
  - `~/Bot/bot.db`
  default config keeps DB in place and survives all container rebuilds/restarts.

Env Separation
--------------
- Compose supports `BOT_ENV_FILE` override (default `.env`).
- CI deploy passes server path (`DEPLOY_ENV_FILE`, default `../.env`) explicitly.
- Recommended layout:
  - server runtime env: `~/Bot/.env`
  - local developer env: `project/.env` (gitignored)
- Deploy never rewrites server env file; it only reads it and fails fast if file is missing.

One-Click Push Commands
-----------------------
- Linux/macOS:
```bash
./bot ship main "feat: your message"
```
- Windows PowerShell:
```powershell
.\ship.ps1 -Branch main -Message "feat: your message"
```

Note About "No Downtime"
------------------------
This bot uses long polling, so only one active bot instance should run at a time. Deployment is graceful and fast, but a short reconnect window (usually a few seconds) may still happen during container replacement.
