FROM golang:1.23

WORKDIR /app

COPY . .

ENV GOPROXY=off \
    GOSUMDB=off \
    CGO_ENABLED=0 \
    COORD_LISTEN=:8080

RUN go build -mod=vendor -o /out/coordination ./cmd/coordination

EXPOSE 8080

CMD ["/out/coordination"]
