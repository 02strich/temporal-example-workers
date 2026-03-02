FROM golang:1.26 AS builder

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/reference/dockerfile/#copy
COPY . ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /container-worker ./cmd/container-worker

FROM gcr.io/distroless/static AS runtime

WORKDIR /

COPY --from=builder /container-worker /container-worker
USER nonroot:nonroot
CMD ["/container-worker"]
