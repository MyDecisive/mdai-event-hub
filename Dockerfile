FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /opt/mdai-event-hub

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /mdai-event-hub ./cmd/mdai-event-hub

FROM gcr.io/distroless/static-debian12
WORKDIR /
COPY --from=builder /mdai-event-hub /mdai-event-hub
CMD ["/mdai-event-hub"]
