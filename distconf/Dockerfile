# ---- build ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# 纯静态二进制，CGO 关闭，便于跑在 distroless/static 上。
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/distconf ./cmd/distconf

# ---- runtime ----
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/distconf /distconf
EXPOSE 9000
ENTRYPOINT ["/distconf", "serve"]

# ---- tools：带 shell 的验证镜像（compose profile=verify 使用） ----
FROM alpine:3.20 AS tools
COPY --from=build /out/distconf /distconf
COPY scripts/verify.sh /verify.sh
ENTRYPOINT ["sh", "/verify.sh"]
