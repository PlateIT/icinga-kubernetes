FROM bit-base-images-docker-hosted.nexus.bit.admin.ch/bit/ca-bundle:latest AS certs
FROM docker-hub.nexus.bit.admin.ch/library/golang:1 AS base

COPY --from=certs /certs/ /usr/local/share/ca-certificates/
RUN update-ca-certificates

# Cache dependencies:
# The go mod download command uses a cache mount,
# ensuring Go modules are cached separately from the build context and not included in image layers.
# This cache is used in the build stage and reused across builds, unless go.mod or go.sum changes.
WORKDIR /build
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

FROM base AS build

# Mount source code and build:
# The --mount=target=. option mounts the source code without adding an extra image layer, unlike `COPY . .`.
# The go build command uses the dependency cache and a dedicated mount to cache build artifacts for future builds.
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /icinga-kubernetes ./cmd/icinga-kubernetes/main.go

FROM scratch

COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --chmod=755 --from=build /icinga-kubernetes /icinga-kubernetes

ENV PATH=/
ENTRYPOINT ["icinga-kubernetes"]
