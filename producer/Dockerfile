FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .
COPY schemas.py .

# RAW_EVENTS_PATH must be a mounted volume in docker-compose,
# not baked into the image. The default in .env is ./data/raw_events
# which maps to /data/raw_events inside the container when volume-mounted.

CMD ["python", "generator.py"]
