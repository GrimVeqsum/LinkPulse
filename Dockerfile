FROM golang:1.25-alpine AS builder

ARG SERVICE

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
  go build \
  -o /out/app \
  ./cmd/${SERVICE}


FROM alpine:3.22

WORKDIR /app

COPY --from=builder /out/app /app/app

CMD ["/app/app"]