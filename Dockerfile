# syntax=docker/dockerfile:1

#####################################################################
# 1️⃣  BUILD STAGE – tool-chain + libuv 1.51 from source + ayder
#####################################################################
FROM ubuntu:22.04 AS build
ARG DEBIAN_FRONTEND=noninteractive

# ---- Build tool-chain ----
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates \
        autotools-dev automake libtool pkg-config \
        dmsetup util-linux zlib1g-dev liburing-dev libcurl4-openssl-dev libssl-dev \
        libevent-dev && \
    rm -rf /var/lib/apt/lists/*

# ---- Build & install libuv 1.51 ----
ARG LIBUV_VERSION=1.51.0
ARG LIBUV_SHA256=5f0557b90b1106de71951a3c3931de5e0430d78da1d9a10287ebc7a3f78ef8eb
RUN curl -fsSL https://dist.libuv.org/dist/v${LIBUV_VERSION}/libuv-v${LIBUV_VERSION}.tar.gz \
      -o /tmp/libuv.tar.gz && \
    echo "${LIBUV_SHA256}  /tmp/libuv.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/libuv.tar.gz -C /tmp && \
    cd /tmp/libuv-v${LIBUV_VERSION} && \
    sh autogen.sh && ./configure && make -j"$(nproc)" && make install && \
    ldconfig && \
    rm -rf /tmp/libuv*

# ---- Compile ayder ----
WORKDIR /app


RUN mkdir -p /data

# Copy build files in a cache-friendly order
COPY Makefile .
COPY deps/picohttpparser ./deps/picohttpparser
COPY src ./src
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Container artifacts must be portable within their target architecture. The
# default developer build intentionally retains -march=native for local
# benchmarking; the portable profile omits host-microarchitecture tuning.
RUN make portable

#####################################################################
# 2️⃣  RUNTIME STAGE – minimal libs + freshly-built libuv
#####################################################################
FROM ubuntu:22.04 AS runtime
ARG AYDER_UID=10001
ARG AYDER_GID=10001
RUN groupadd --system --gid "${AYDER_GID}" ayder && \
    useradd --system --uid "${AYDER_UID}" --gid "${AYDER_GID}" --home-dir /app --shell /usr/sbin/nologin ayder && \
    mkdir -p /data /app && chown -R ayder:ayder /data /app

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        dmsetup util-linux zlib1g liburing2 \
        ca-certificates libcurl4 libssl3 libevent-2.1-7 \
        iproute2 iptables curl && \
    rm -rf /var/lib/apt/lists/*

# copy our glibc-compatible libuv
COPY --from=build /usr/local/lib/libuv.so* /usr/local/lib/
RUN ldconfig                                  # refresh linker cache

# copy binary + entrypoint
WORKDIR /app
COPY --chown=ayder:ayder --from=build /app/build/portable/ayder ./ayder
COPY --chown=ayder:ayder --from=build /app/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

EXPOSE 1109
USER ${AYDER_UID}:${AYDER_GID}
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:1109/ready | grep -Eq '"ready"[[:space:]]*:[[:space:]]*true' || exit 1
ENTRYPOINT ["./entrypoint.sh"]
CMD ["./ayder", "--workers", "1"]
