FROM alpine:3.10
MAINTAINER venjiang <venjiang@gmail.com>
RUN adduser -D developer
WORKDIR /app
COPY pg-listener .
USER developer

ENTRYPOINT ["./pg-listener"]
