FROM gcr.io/distroless/base-debian12:latest
LABEL org.opencontainers.image.source=https://github.com/kasplex/go-krc20d

WORKDIR /app
COPY krc20d /app/krc20d

ENTRYPOINT ["/app/krc20d"]
