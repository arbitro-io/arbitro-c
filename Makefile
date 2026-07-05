CC      ?= cc
CFLAGS  ?= -std=c99 -O2 -Wall -Wextra -Wpedantic -Werror
LDFLAGS ?=
PREFIX  ?= /usr/local

INCLUDES = -Iinclude
SRC      = src/arbitro.c
OBJ      = build/arbitro.o

UNAME := $(shell uname -s 2>/dev/null || echo Windows)
ifneq (,$(findstring MINGW,$(UNAME)))
  LDFLAGS += -lws2_32
else ifneq (,$(findstring MSYS,$(UNAME)))
  LDFLAGS += -lws2_32
else ifeq ($(UNAME),Windows)
  LDFLAGS += -lws2_32
else ifeq ($(OS),Windows_NT)
  LDFLAGS += -lws2_32
else
  CFLAGS += -D_POSIX_C_SOURCE=200112L
endif

.PHONY: all clean install test examples benchmarks

all: build/libarbitro.a

build/libarbitro.a: $(OBJ)
	ar rcs $@ $^

build/arbitro.o: $(SRC) include/arbitro/arbitro.h | build
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

build:
	mkdir -p build

# --- Tests ---

test: build/unit_tests
	./build/unit_tests

build/unit_tests: tests/unit_tests.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/integration: tests/integration.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

# --- Examples ---

examples: build/example_pubsub build/example_service

build/example_pubsub: examples/pubsub.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/example_service: examples/service.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

# --- Benchmarks ---

benchmarks: build/bench_publish build/bench_roundtrip build/bench_throughput build/bench_limits build/bench_chaos

build/bench_publish: benchmarks/bench_publish.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/bench_roundtrip: benchmarks/bench_roundtrip.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/bench_throughput: benchmarks/bench_throughput.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/bench_limits: benchmarks/bench_limits.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

build/bench_chaos: benchmarks/bench_chaos.c build/libarbitro.a | build
	$(CC) $(CFLAGS) $(INCLUDES) $< -Lbuild -larbitro $(LDFLAGS) -o $@

# --- Install ---

install: build/libarbitro.a
	install -d $(PREFIX)/lib $(PREFIX)/include/arbitro
	install -m 644 build/libarbitro.a $(PREFIX)/lib/
	install -m 644 include/arbitro/arbitro.h $(PREFIX)/include/arbitro/

# --- Clean ---

clean:
	rm -rf build
