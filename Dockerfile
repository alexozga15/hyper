FROM python:3.13-slim

WORKDIR /app

COPY server.py /app/server.py
COPY static /app/static
COPY data /app/data

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV PORT=8000

EXPOSE 8000

CMD ["python", "server.py"]
