# Shopping Sessions dev targets. The scorer's docker flags live in scripts/docker_run.sh.
SHELL      := /bin/bash
IMAGE      ?= shopping-sessions:latest
CPUS       ?= 8
MEM        ?= 24g
PLATFORM   ?= linux/amd64
# x86-64-v3 (AVX2) runs on the laptop and the scorer; cascadelake is only a late experiment
TARGET_CPU ?= x86-64-v3
# BIN=reference or BIN=shopping overrides the Dockerfile default
BIN        ?=
PY         ?= .venv/bin/python
REF        ?= target/release/reference
GEN        := kit/bin/generate
export IMAGE CPUS MEM PLATFORM PY REF

TINY_SEEDS   := $(shell seq 101 110)
MEDIUM_SEEDS := 2 3

.PHONY: kit data data60 build ref fixtures ref-fixtures run-dev check-dev ref-dev crosscheck check20 check60 bench20 bench60

## Official generator + dev answer into kit/ (git-ignored)
kit:
	bash scripts/fetch_kit.sh

$(GEN):
	bash scripts/fetch_kit.sh

## Local datasets into data/ (git-ignored)
data: $(GEN)
	mkdir -p data/x
	[ -d data/dev ] || $(GEN) --seed 1 --events 1000000 --clicks 250000 --output data/dev
	for s in $(TINY_SEEDS); do [ -d data/x/s$$s ] || $(GEN) --seed $$s --events 20000 --clicks 5000 --output data/x/s$$s; done
	for s in $(MEDIUM_SEEDS); do [ -d data/x/s$$s ] || $(GEN) --seed $$s --events 500000 --clicks 125000 --output data/x/s$$s; done
	[ -d data/p20m ] || $(GEN) --seed 7 --events 20000000 --clicks 5000000 --output data/p20m

## Final-size dataset (about 5.7 GB)
data60: $(GEN)
	[ -d data/f60m ] || $(GEN) --seed 11 --events 60000000 --clicks 15000000 --output data/f60m

## Docker image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --build-arg TARGET_CPU=$(TARGET_CPU) $(if $(BIN),--build-arg BIN=$(BIN)) --load -t $(IMAGE) .

## Reference binary, built natively (it is the oracle)
ref:
	cargo build --release --bin reference

## 36 fixtures through the Docker image with the scorer's flags
fixtures: build
	$(PY) scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## 36 fixtures through the native reference binary
ref-fixtures: ref
	$(PY) scripts/run_fixtures.py fixtures -- $(REF)

## Public dev set through the image, then compare with the official answer
run-dev: build
	rm -rf out/dev && mkdir -p out && time scripts/docker_run.sh data/dev out/dev

check-dev:
	$(PY) scripts/compare.py out/dev kit/dev-answer

## Public dev set through the reference, compared with the official answer
ref-dev: ref
	rm -rf out/ref-dev && mkdir -p out/ref-dev
	DATA_DIR=data/dev OUTPUT_DIR=out/ref-dev $(REF) data/dev out/ref-dev
	$(PY) scripts/compare.py out/ref-dev kit/dev-answer

## Image vs reference: small generated sets, 20M set, 60M set
crosscheck: build ref
	bash scripts/crosscheck.sh

check20: build ref
	bash scripts/crosscheck.sh data/p20m

check60: build ref data60
	bash scripts/crosscheck.sh data/f60m

## Timed runs with the scorer's flags
bench20: build
	bash scripts/bench.sh data/p20m 3

bench60: build data60
	bash scripts/bench.sh data/f60m 1
