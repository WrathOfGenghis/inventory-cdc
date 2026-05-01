# Multi-stage build for the inventory-cdc orchestrator.
#
# Stage 1: build the Go binary against the Go 1.22 toolchain.
# Stage 2: copy the static binary onto a distroless base. The resulting
#          image is < 25 MB and has no shell, package manager, or
#          libc surface area for an attacker to land on.

FROM golang:1.22-alpine AS builder

WORKDIR /src

# Cache module downloads in their own layer.
COPY go.mod go.sum ./
RUN go mod download

# Now bring in the source and build.
COPY . .

# CGO disabled → fully static binary that runs on distroless:static.
# Trim symbol table and DWARF info to keep the image small.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
        -o /out/orchestrator ./cmd/orchestrator

# ---------------------------------------------------------------------------

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/orchestrator /app/orchestrator
COPY schema/contracts /app/schema/contracts

# Distroless runs as user `nonroot` (uid 65532) by default.
USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/app/orchestrator"]
