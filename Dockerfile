FROM python:3.13-slim

WORKDIR /app

# server.py imports coinmarketman and moni at module level, and the alerting
# entrypoints live in scripts/ -- all of them must be in the image.
COPY server.py coinmarketman.py moni.py /app/
COPY scripts /app/scripts
COPY static /app/static
COPY data /app/data

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV PORT=8000

# Fail the build rather than the deploy if an import is missing.
RUN python -c "import server, coinmarketman, moni"

RUN useradd --create-home --uid 10001 app && chown -R app:app /app
USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,os,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8000')+'/api/health',timeout=4).status==200 else 1)"

CMD ["python", "server.py"]
