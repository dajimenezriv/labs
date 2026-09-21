# Only the exporter runs in a container. The producer and the consumer group
# run on the host in one process, because every variant starts members on a
# schedule and measures them from the inside.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -o /hp .

FROM alpine:3.22
COPY --from=build /hp /hp
ENTRYPOINT ["/hp"]
