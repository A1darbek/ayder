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
        dmsetup util-linux zlib1g-dev liburing-dev libcurl4-openssl-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# ---- Build & install libuv 1.51 ----
ARG LIBUV_VERSION=1.51.0
RUN curl -fsSL https://dist.libuv.org/dist/v${LIBUV_VERSION}/libuv-v${LIBUV_VERSION}.tar.gz \
      -o /tmp/libuv.tar.gz && \
    tar -xzf /tmp/libuv.tar.gz -C /tmp && \
    cd /tmp/libuv-v${LIBUV_VERSION} && \
    sh autogen.sh && ./configure && make -j"$(nproc)" && make install && \
    ldconfig && \
    rm -rf /tmp/libuv*

# ---- Compile ayder ----
WORKDIR /app


# Always ensure picohttpparser exists; overlay later if repo has it
RUN mkdir -p deps/picohttpparser && \
    curl -fsSL https://raw.githubusercontent.com/h2o/picohttpparser/master/picohttpparser.c \
      -o deps/picohttpparser/picohttpparser.c && \
    curl -fsSL https://raw.githubusercontent.com/h2o/picohttpparser/master/picohttpparser.h \
      -o deps/picohttpparser/picohttpparser.h

RUN mkdir -p /data

# Copy build files in a cache-friendly order
COPY Makefile .
COPY deps/picohttpparser ./deps/picohttpparser
COPY src ./src
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

RUN make clean && make

#####################################################################
# 2️⃣  RUNTIME STAGE – minimal libs + freshly-built libuv
#####################################################################
FROM ubuntu:22.04 AS runtime
RUN mkdir -p /data

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        dmsetup util-linux zlib1g liburing2 && \
    rm -rf /var/lib/apt/lists/*

# copy our glibc-compatible libuv
COPY --from=build /usr/local/lib/libuv.so* /usr/local/lib/
RUN ldconfig                                  # refresh linker cache

# copy binary + entrypoint
WORKDIR /app
COPY --from=build /app/ayder      ./ayder
COPY --from=build /app/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

EXPOSE 1109
ENTRYPOINT ["./entrypoint.sh"]
CMD ["./ayder", "--workers", "1"]
