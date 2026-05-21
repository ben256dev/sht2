.PHONY: build rebuild clean test

build:
	mkdir -p bin
	go build -o bin/shtd ./cmd/shtd
	go build -o bin/sht-shell ./cmd/sht-shell

rebuild: clean build

clean:
	rm -f bin/shtd bin/sht-shell

test:
	go test ./...
