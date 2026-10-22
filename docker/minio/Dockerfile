# =============================================================================
#  MinIO compilé depuis les sources, à une release figée.
#  Pourquoi : depuis fin 2025, MinIO ne publie plus d'images publiques
#  (docker.io/minio/minio et quay.io/minio/minio exigent une authentification).
#  Compiler une release taguée garantit un `make up` reproductible sur
#  n'importe quelle machine, sans dépendre d'un registre externe.
# =============================================================================
FROM golang:1.24-bookworm AS build

ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" https://github.com/minio/minio.git /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/minio .

# -----------------------------------------------------------------------------
FROM debian:bookworm-slim

# curl : nécessaire au healthcheck (/minio/health/live)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/minio /usr/local/bin/minio

EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
