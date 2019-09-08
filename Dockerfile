FROM golang:latest as builder
WORKDIR /app
COPY go.mod go.sum ./
COPY . .
RUN go clean
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o crashplan-ffs-puller .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN mkdir -p etc/crashplan-ffs-puller
RUN mkdir -p /crashplan-ffs-puller && chown -R nobody:nogroup etc/crashplan-ffs-puller /crashplan-ffs-puller

USER nobody

VOLUME ["/crashplan-ffs-puller"]
WORKDIR /crashplan-ffs-puller
COPY --from=builder /app/crashplan-ffs-puller /bin/crashplan-ffs-puller

ENTRYPOINT ["/bin/crashplan-ffs-puller"]
CMD ["--config=/etc/crashplan-ffs-puller/config.json"]