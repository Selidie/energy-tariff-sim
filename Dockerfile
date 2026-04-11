FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ARG GIT_VERSION=dev
ARG BUILD_TIME=
ENV APP_VERSION=${GIT_VERSION}
ENV BUILD_TIME=${BUILD_TIME}
ENV PORT=5011
ENV CONFIG_PATH=/app/config/settings.yaml
EXPOSE 5011
CMD ["python", "-m", "app.api"]
