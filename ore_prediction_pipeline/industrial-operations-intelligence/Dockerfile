FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /workspace
RUN pip install --upgrade pip

COPY . .
RUN pip install -e ".[dev]"

ENV PYTHONUNBUFFERED=1
