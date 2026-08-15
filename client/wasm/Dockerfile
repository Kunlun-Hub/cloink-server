# syntax=docker/dockerfile:1.7

FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=js GOARCH=wasm go build \
    -trimpath -ldflags="-s -w" \
    -o /out/netbird.wasm ./client/wasm/cmd && \
    cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" /out/wasm_exec.js

FROM nginx:1.29-alpine

COPY client/wasm/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /out/ /usr/share/nginx/html/

EXPOSE 80

