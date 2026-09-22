# syntax=docker/dockerfile:1

FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY shared ./shared
COPY gateway ./gateway

ARG BINARY
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/service ./gateway/cmd/${BINARY}

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/service /service

USER nonroot:nonroot
ENTRYPOINT ["/service"]
