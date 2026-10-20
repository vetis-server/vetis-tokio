FROM rust:alpine AS build

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache lld mold musl musl-dev libc-dev cmake clang-static llvm-static openssl file \
        libressl-dev git make build-base bash curl wget zip gnupg coreutils gcc g++ zstd pkgconfig \
    binutils ca-certificates upx

WORKDIR /docker
COPY . ./
RUN cd /docker/vetis && \
    cargo build --release --features="http2 http3 rust-tls" \
    --no-default-features --target=x86_64-unknown-linux-musl


FROM alpine:latest AS files

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache ca-certificates mailcap tzdata

RUN update-ca-certificates

ENV USER=vetis
ENV UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/var/www/vetis" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"


FROM scratch

COPY --from=files --chmod=444 \
    /etc/passwd \
    /etc/group \
    /etc/nsswitch.conf \
    /etc/mime.types \
    /etc/

COPY --from=files --chmod=444 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=files --chmod=444 /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=build /docker/target/x86_64-unknown-linux-musl/release/vetis /bin/vetis

USER vetis:vetis

WORKDIR /app

ENTRYPOINT ["/bin/vetis"]
