# Changelog

All notable changes to `arbitro-c` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/). This client versions
independently of the broker and its sibling clients — it is younger, and its
numbers do not track theirs.

## [Unreleased]

### Breaking

- **`arbitro_msg_t.subject_hash` is now `sub_id`**, and likewise on
  `arbitro_msg_owned_t`. Code reading the old field name no longer compiles.

  This corrects a name, not a layout: offset +4 of `DeliverBody` has always
  carried the id of the subscription a copy was delivered for (see
  `arbitro-proto` `deliver_frame.rs`). The wrong name mattered because an ack
  must echo that id — the broker opens one pending per subscription, so acking
  under another subscription's id leaves the real one outstanding until
  `ack_wait` fires.

### Added

- **Client-owned subscription ids and local fan-out.** The broker sends one
  wire copy per `(connection, consumer)`, stamped with whichever subscription
  matched; siblings sharing that consumer never see their own copy. The client
  now allocates its own ids (from 1; 0 means "unset" to the broker), routes
  deliveries by subscription id, and fans each one out to every sibling whose
  filter accepts the subject. A lone subscription skips the filter check.
  Resubscribing after a reconnect sends the same id it was given, rather than
  the hardcoded 0 it used to send.

  `arbitro_subscribe_filter()` is how a subscription declares a filter of its
  own. It must be COVERED by its consumer's filter, or nothing is delivered.

### Fixed

- **The integration suite ran again.** Most of it claimed `>` as its stream
  filter, which the broker has refused since catalog admission rules landed —
  so `stream_upsert` failed and the failure read downstream as "nothing was
  delivered". 111 checks across seven files were failing for that reason
  alone, none of them for a client defect. Filters and subjects are now scoped
  under each test's own stream.
- **`bench_limits` measured nothing**, for the same reason, in all four
  stages.

### Known gaps

- No coverage of reconnect behaviour or malformed-frame handling. Thirteen
  checks name those cases and skip: five need a harness that kills the broker
  out of process, seven need a mock broker that emits corrupt frames, one
  needs a pull-mode API the header does not expose.
