FROM golang:1.26.5 AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o /app
 
FROM gcr.io/distroless/static
COPY --from=build /app /app
ENTRYPOINT ["/app"]