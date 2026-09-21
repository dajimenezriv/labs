# Only the exporter runs in a container. The producer and consumer stay on the
# host, because every crash variant kills them by pid and restarts them, and
# doing that through compose would measure container restart time rather than
# consumer group recovery time.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -o /dg .

FROM alpine:3.22
COPY --from=build /dg /dg
ENTRYPOINT ["/dg"]
