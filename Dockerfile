FROM python:3.11-slim

# 讓 Python 的輸出訊息即時顯示
ENV PYTHONUNBUFFERED=1 
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv 

COPY . .
RUN uv sync --frozen --no-dev

ENTRYPOINT ["uv", "run", "uvicorn", "app:app"]
