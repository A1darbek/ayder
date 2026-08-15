FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 benchmark && \
    useradd --system --uid 10001 --gid 10001 --home-dir /bench \
      --shell /usr/sbin/nologin benchmark

WORKDIR /bench

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=benchmark:benchmark bench.py .

USER 10001:10001
ENTRYPOINT ["python", "/bench/bench.py"]
