# Multi-stage build: compile in the full Go image, then copy just the binaries into a
# tiny distroless runtime (no shell, no toolchain — small image, small attack surface).
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -o /simulator ./cmd/simulator

FROM gcr.io/distroless/static-debian12
COPY --from=build /gateway /gateway
COPY --from=build /simulator /simulator
ENTRYPOINT ["/gateway"]
