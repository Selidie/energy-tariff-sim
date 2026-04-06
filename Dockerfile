FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=5010
ENV CONFIG_PATH=/app/config/settings.yaml
EXPOSE 5010
CMD ["python", "-m", "app.api"]
