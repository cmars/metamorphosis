VERSION := $(shell awk '/version:/ {print $$2}' snap/snapcraft.yaml | head -1 | sed "s/'//g")

.PHONY: all
all: snap charm

.PHONY: snap
snap: kpi-exporter_$(VERSION)_amd64.snap

kpi-exporter_$(VERSION)_amd64.snap:
	snapcraft

.PHONY: charm
charm: charm/builds/kpi-exporter

charm/builds/kpi-exporter:
	$(MAKE) -C charm/kpi-exporter

.PHONY: lint
lint:
	flake8 --ignore=E121,E123,E126,E226,E24,E704,E265 charm/kpi-exporter

.PHONY: docker-image
docker-image:
	$(MAKE) -C kpi-exporter all

.PHONY: clean
clean: clean-charm clean-snap clean-docker

.PHONY: clean-charm
clean-charm:
	$(RM) -r charm/builds/kpi-exporter

.PHONY: clean-snap
clean-snap:
	snapcraft clean

.PHONY: clean-docker
clean-docker:
	$(MAKE) -C kpi-exporter clean
