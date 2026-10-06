FROM python:3.12-slim

# libopus is required for voice. Render's native Python runtime can't apt-get, so deploy with Docker.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libopus0 \
    && rm -rf /var/lib/apt/lists/*

# MALLOC_ARENA_MAX=2 stops glibc from creating a memory arena per thread (voice uses several threads).
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MALLOC_ARENA_MAX=2

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY discord_bot_voice.py .
CMD ["python", "discord_bot_voice.py"]
