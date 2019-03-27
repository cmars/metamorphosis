.PHONY: all
all: exporter Dockerfile
	docker build -t exporter .

exporter: ../go.mod ../go.sum *.go
	go build .

.PHONY: clean
clean:
	$(RM) exporter
