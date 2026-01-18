# PMD VPS Deployment (Docker Compose + Caddy)

This runbook deploys the PMD frontend (Next.js) and backend (FastAPI + worker + scheduler)
on a single VPS with HTTPS via Caddy. Production assets live under `pmd_infra/`.

## Provision the VPS

Example for Ubuntu 22.04:

1) Create a deploy user and add SSH key:

```bash
adduser pmd
usermod -aG sudo pmd
mkdir -p /home/pmd/.ssh
```

2) Firewall:

```bash
ufw allow OpenSSH
ufw allow 80
ufw allow 443
ufw enable
```

3) Install Docker + Compose plugin:

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
usermod -aG docker pmd
```

Log out/in so the `pmd` user can run Docker.

## DNS setup

Create A records:

- `app.example.com` -> VPS IP
- `api.example.com` -> VPS IP

## Configure environment

Ensure the frontend and infra repos sit beside the backend repo:

```bash
mkdir -p /srv/pmd-stack
cd /srv/pmd-stack
git clone <backend-repo-url> pmd
git clone <frontend-repo-url> pmd_frontend
git clone <infra-repo-url> pmd_infra
cd pmd_infra
```

From the infra repo root (`pmd_infra/`):

```bash
cp env/prod.env.example .env
```

Fill in all values in `.env`. Notes:

- `NEXT_PUBLIC_*` variables are public and bundled into the frontend.
- All other secrets (Stripe, Telegram, admin API key, session secret) are server-only.
- `APP_DOMAIN`/`API_DOMAIN` must match your DNS records.
- `APP_URL` should be `https://app.example.com`.
- `NEXT_PUBLIC_API_BASE_URL` should be `https://api.example.com`.

## First deploy

```bash
mkdir -p backups
chmod +x scripts/*.sh
./scripts/deploy.sh
```

If you want to test Let's Encrypt staging first, uncomment the staging `acme_ca`
line in `caddy/Caddyfile`, deploy once, then comment it back for production.
## Updating

```bash
git pull
./scripts/deploy.sh
```

This pulls, rebuilds, and restarts services with minimal downtime.

## Backups

```bash
./scripts/backup.sh
```

Backups are saved to `backups/` and rotated (last 7 by default).

### Restore notes

1) Stop services:

```bash
docker compose -f compose/compose.prod.yml --env-file .env down
```

2) Restore:

```bash
cat backups/<file>.sql | docker compose -f compose/compose.prod.yml --env-file .env exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"
```

3) Start services:

```bash
./scripts/deploy.sh
```

## Troubleshooting checklist

- `docker compose -f compose/compose.prod.yml --env-file .env ps`
- `docker compose -f compose/compose.prod.yml --env-file .env logs --tail=200 proxy`
- Confirm DNS A records resolve to the VPS IP.
- Ensure ports 80/443 are open.
- Run `./scripts/deploy.sh` for health checks.
