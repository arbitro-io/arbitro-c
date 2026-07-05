# arbitro-c

Single-file C client for the Arbitro message broker. Targets C99, zero external dependencies, embeddable in any project.

## Rules

Read all `.claude/rules/*.md` before writing or modifying any code. Rules are INVIOLABLE.

## Language

- C99 (`-std=c99`), POSIX-compatible, Windows via Winsock2
- No C++ constructs, no compiler extensions unless `#ifdef`-guarded
- All public symbols prefixed `arbitro_` or `ARBITRO_`

## Code Style

- **ZERO comments unless the WHY is non-obvious.** Never comment what the code does.
- No multi-line comment blocks. One line max, only for hidden constraints.
- No TODO/FIXME in committed code — use TODO.md instead.
- Functions under 40 lines. Split if longer.
- No heap allocation on the hot path (publish, deliver, ack).
- `static` everything that isn't in the public API.
- Prefer stack buffers with known bounds over malloc.

## Performance

- Hot path: publish, deliver callback, ack — must be zero-alloc.
- Cold path: create_stream, create_consumer — may allocate (JSON emit).
- No `memcpy` where pointer arithmetic suffices.
- Batch acks: accumulate and flush, never one syscall per ack.
- Use `writev`/`WSASend` for scatter-gather when possible.

## Build

```bash
# CMake (full)
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Make (quick, no CMake needed)
make

# Single-file embed (just copy these two files)
cp include/arbitro/arbitro.h your_project/
cp src/arbitro.c your_project/
```

## Testing

```bash
# Unit tests (no broker)
cmake --build build --target test

# Integration tests (broker on 127.0.0.1:9898)
ARBITRO_ADDR=127.0.0.1:9898 ./build/tests/integration
```

## Wire Protocol

Read `.claude/rules/wire-protocol.md` for the complete frame format reference.
The source of truth is always `arbitro-proto` in the broker repo.
