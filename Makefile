.PHONY: build install restart deploy rebuild clean test

build:
	mkdir -p bin
	go build -o bin/shtd ./cmd/shtd
	go build -o bin/sht-shell ./cmd/sht-shell
	go build -o bin/sht-admin ./cmd/sht-admin
	go build -o bin/sht-gateway ./cmd/sht-gateway

install: build
	install -m 0755 bin/shtd /usr/local/bin/shtd
	install -m 0755 bin/sht-shell /usr/local/bin/sht-shell
	install -m 0755 bin/sht-admin /usr/local/bin/sht-admin
	install -m 0755 bin/sht-gateway /usr/local/bin/sht-gateway

restart:
	systemctl restart shtd

deploy: install restart

rebuild: clean build

clean:
	rm -f bin/shtd bin/sht-shell bin/sht-admin bin/sht-gateway

test:
	go test ./...
