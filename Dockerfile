FROM golang:latest as builder
WORKDIR /app
COPY go.mod go.sum ./
COPY . .
RUN go clean
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o crashplan-ffs-puller .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /usr/local/crashplan-ffs-puller
COPY --from=builder /app/crashplan-ffs-puller .

CMD ["/usr/local/crashplan-ffs-puller/crashplan-ffs-puller"]