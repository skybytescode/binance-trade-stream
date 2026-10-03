FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags='-s' -o /out/tradestream ./cmd/tradestream

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tradestream /tradestream
EXPOSE 8090
ENTRYPOINT ["/tradestream", "-table=false"]
