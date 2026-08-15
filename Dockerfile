# syntax=docker/dockerfile:1.7
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/aero-arc-conformance ./cmd/aero-arc-conformance

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/aero-arc-conformance /usr/local/bin/aero-arc-conformance
EXPOSE 2112 50052
ENTRYPOINT ["/usr/local/bin/aero-arc-conformance"]
CMD ["--config-path", "/etc/aero-arc/config.yaml"]
