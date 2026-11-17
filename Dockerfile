FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    NEWS_AGENT_HOST=0.0.0.0 \
    NEWS_AGENT_DB=/data/news_agent.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agenticai.py .

# run as a normal user; /data holds the SQLite file (mount a volume here to keep it)
RUN useradd --create-home app && mkdir -p /data && chown -R app:app /app /data
USER app

EXPOSE 8000
# the app reads $PORT when the host sets one, otherwise 8000
CMD ["python", "agenticai.py", "--no-browser"]
