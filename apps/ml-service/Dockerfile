FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 10001 app && useradd -m -u 10001 -g app app

COPY requirements.txt ./
COPY requirements.lock.txt* ./
RUN if [ -f requirements.lock.txt ]; then \
      pip install --no-cache-dir -r requirements.lock.txt; \
    else \
      pip install --no-cache-dir -r requirements.txt; \
    fi

COPY --chown=app:app . .

USER app

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8112"]
