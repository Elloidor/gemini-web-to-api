# Stage 1: Build
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o main ./cmd/server/main.go

# Stage 2: Final Image
FROM golang:1.25-alpine

# Create a non-root user and group
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /home/appuser

# Copy file from builder and change ownership
COPY --from=builder --chown=appuser:appgroup /app/main .

# Switch to non-root user
USER appuser

EXPOSE 4981

ENTRYPOINT ["./main"] 
