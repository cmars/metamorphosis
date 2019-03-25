.PHONY: all
all: kpi-exporter Dockerfile
	docker build -t kpi-exporter .

kpi-exporter: ../../go.mod ../../go.sum *.go
	go build .

.PHONY: clean
clean:
	$(RM) kpi-exporter
