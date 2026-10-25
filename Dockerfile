FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 METRICS_MODE=instance
WORKDIR /app
COPY requirements.lock .
RUN python -m pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config
COPY migrations ./migrations
RUN python -m pip install --no-deps --no-build-isolation . \
    && useradd --create-home --uid 10001 app
USER 10001:10001
EXPOSE 8000 8001
ENTRYPOINT ["python", "-u", "-m"]
CMD ["composition.api_main"]

FROM runtime AS test
COPY tests ./tests
