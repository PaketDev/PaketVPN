FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN useradd --create-home --shell /usr/sbin/nologin app \
    && mkdir -p /data /app/project \
    && chown -R app:app /data /app

COPY requirements.txt /app/project/requirements.txt
RUN pip install --no-cache-dir -r /app/project/requirements.txt

COPY . /app/project
RUN chown -R app:app /app/project

USER app

VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD python -c "import os,urllib.request; port=os.getenv('HEALTH_CHECK_PORT','8080'); urllib.request.urlopen(f'http://127.0.0.1:{port}/healthcheck', timeout=3).read()"

CMD ["python", "-m", "project.app.main"]
