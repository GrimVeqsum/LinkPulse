ARG GO_VERSION=1.26.8
FROM golang:${GO_VERSION}-alpine AS source

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS integration-tests
ENV CGO_ENABLED=0
CMD ["go", "test", "-tags=integration", "./tests/integration", "-count=1", "-v", "-timeout=3m"]

FROM source AS builder
ARG SERVICE
RUN test "$SERVICE" = link-service -o "$SERVICE" = analytics-service
RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app app
WORKDIR /app
COPY --from=builder --chown=app:app /out/app /app/app
USER 10001:10001
ENTRYPOINT ["/app/app"]
