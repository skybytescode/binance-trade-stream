## run: follow BTC, ETH, SOL, BNB, XRP and ADA with a live terminal table
run:
	go run ./cmd/tradestream

## test: run all tests (no network needed)
test:
	go test -race ./...

## docker: build and run the API in a container on :8090
docker:
	docker build -t tradestream . && docker run --rm -p 8090:8090 tradestream

.PHONY: run test docker
