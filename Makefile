VERSION := $(shell awk '/version:/ {print $$2}' snap/snapcraft.yaml | head -1 | sed "s/'//g")

.PHONY: all
all: snap charm

.PHONY: snap
snap: metamorphosis_$(VERSION)_amd64.snap

metamorphosis_$(VERSION)_amd64.snap:
	snapcraft --use-lxd

.PHONY: charm
charm: charm/builds/metamorphosis

charm/builds/metamorphosis:
	$(MAKE) -C charm/metamorphosis

.PHONY: lint
lint:
	flake8 --ignore=E121,E123,E126,E226,E24,E704,E265 charm/metamorphosis

.PHONY: docker-image
docker-image:
	$(MAKE) -C metamorphosis all

.PHONY: clean
clean: clean-charm clean-snap clean-docker

.PHONY: clean-charm
clean-charm:
	$(RM) -r charm/builds/metamorphosis

.PHONY: clean-snap
clean-snap:
	snapcraft clean
	rm -f metamorphosis_$(VERSION)_amd64.snap

.PHONY: clean-docker
clean-docker:
	$(MAKE) -C exporter clean
