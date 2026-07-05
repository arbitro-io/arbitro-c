# Code Style Rules

## Naming

- Public API: `arbitro_<noun>_<verb>()` — e.g. `arbitro_client_connect()`, `arbitro_msg_ack()`
- Internal: `arb__<module>_<verb>()` — double underscore prefix for internal linkage
- Types: `arbitro_<noun>_t` — e.g. `arbitro_client_t`, `arbitro_msg_t`
- Constants/macros: `ARBITRO_<NOUN>` — e.g. `ARBITRO_MAX_SUBJECT_LEN`
- Enum values: `ARBITRO_<ENUM>_<VALUE>` — e.g. `ARBITRO_ACK_EXPLICIT`

## Comments

1. Default: write NO comments.
2. One-line comment ONLY when the WHY is non-obvious (hidden constraint, workaround, platform quirk).
3. Never comment WHAT the code does — naming must be self-documenting.
4. Never reference tickets, PRs, or callers in comments.
5. No banners, section separators, or decorative comments.
6. No `// end if`, `// end for`, `// end function` noise.

## Functions

- Max 40 lines per function body. Split at logical boundaries.
- One return point preferred; early returns for guard clauses are OK.
- Parameters: output params last, prefixed `out_`.
- Error return: `int` (0 = success, negative = error code from `arbitro_err.h`).

## Structs

- All structs are opaque to consumers (forward-declared in header, defined in .c).
- Exception: `arbitro_msg_t` is a view struct (no ownership) — may be stack-allocated.
- No bitfields in wire-facing structs (portability).

## Memory

- Caller owns buffers for output (pass ptr + len).
- Library never calls `malloc` on the hot path.
- Cold path allocations: paired with a `_free()` or documented as caller-freed.
- No global mutable state. All state in `arbitro_client_t`.

## Includes

- Public headers: `#include <arbitro/arbitro.h>` only.
- Internal: `#include "arb_internal.h"` — never exposed.
- System headers: grouped and sorted. POSIX first, then platform-specific `#ifdef`.

## Error Handling

- All fallible functions return `int` (error code).
- `ARBITRO_OK` = 0, all errors < 0.
- No errno abuse. No setjmp/longjmp. No signal handlers in library code.
