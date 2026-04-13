FROM python:3.11-slim

# ── System dependencies ────────────────────────────────────────────────────
# Install Docker CLI so sa_import.py can spin up a temporary InfluxDB 1.x
# container when processing Solar Assistant backups.  We only need the
# client binary (docker-ce-cli), not the full Docker Engine daemon.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
        https://download.docker.com/linux/debian \
        $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && apt-get purge -y --auto-remove curl gnupg \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ───────────────────────────────────────────────────────
COPY . .

ARG GIT_VERSION=dev
ARG BUILD_TIME=
ENV APP_VERSION=${GIT_VERSION}
ENV BUILD_TIME=${BUILD_TIME}
ENV PORT=5011
ENV CONFIG_PATH=/app/config/settings.yaml

EXPOSE 5011
CMD ["python", "-m", "app.api"]
