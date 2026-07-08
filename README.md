# Stream Processing Engine

A lock-free, multi-stage stream processing engine written in C++20. Events
flow through bounded single-producer/single-consumer (SPSC) rings between
pipeline stages (generate -> transform -> windowed aggregate), with
backpressure propagating upstream when a stage falls behind.

## Layout

include/spe/spsc_ring.hpp    SPSC lock-free ring buffer:
                              - SpscRingNaive: per-event atomic ops
                              - SpscRing: index caching + batched publication
include/spe/mutex_queue.hpp  Mutex-guarded bounded queue (comparison baseline)
include/spe/event.hpp        32-byte POD event + tumbling-window aggregation
include/spe/pipeline.hpp     3-stage pipeline with backpressure
bench/bench_main.cpp         Benchmark harness (throughput + latency percentiles)
tests/tests.cpp              Correctness tests (run under TSan and ASan)

## Build and run

mkdir build && cd build
cmake .. && make
./tests               # correctness
./bench 20000000      # benchmark, 20M events

Sanitizer builds:

mkdir build-tsan && cd build-tsan && cmake .. -DSANITIZE=thread && make tests && ./tests
mkdir build-asan && cd build-asan && cmake .. -DSANITIZE=address && make tests && ./tests

## Design

SPSC over MPMC. Each ring index has exactly one writer, so plain
load/store with acquire/release ordering is sufficient, with no CAS loops
and no ABA problem. Multi-producer scenarios are handled by composing
multiple SPSC rings (one per producer) rather than a single MPMC structure.

Memory ordering. The producer writes the slot then publishes with
memory_order_release; the consumer reads the index with memory_order_acquire
before reading the slot. This gives the one guarantee needed, that slot
writes happen-before the index publish, without paying for the global
ordering seq_cst would additionally impose.

Batched publication. The optimised ring caches the other side's index
locally and only re-loads it when the cached view suggests full or empty,
and publishes once per batch rather than once per event. Cross-core atomic
operations force cache-line ownership transfers between cores; batching
amortises that synchronisation cost across the batch instead of paying it
per event.

Cache-line alignment. Head and tail are written by different cores and are
aligned to separate cache lines to avoid false sharing, since otherwise a
write to one would invalidate the other core's cached copy of the
neighbouring, logically independent, variable.

Backpressure. Bounded rings with a spinning or yielding producer: overload
slows the source rather than dropping events. A drop-and-count policy is a
one-line alternative in the push loop for cases where freshness matters
more than completeness.

## Benchmark methodology

Warmup pass before every measured configuration. Median of 5 repetitions
reported for throughput. Latency measured in a separate run from
throughput, sampling 1 event in 64 to avoid perturbing the measurement.
Percentiles (p50/p99/p99.9) rather than means, since latency is
heavy-tailed. The mutex-queue baseline shares ring storage, indexing,
event type, and thread structure with the lock-free versions; only the
synchronisation strategy differs.

Numbers should be measured on the target machine rather than in a
container or VM, since shared cores and scheduling noise are not
representative.

## Notes

During development, ThreadSanitizer flagged a data race where the
producer and transformer threads both incremented one shared stall
counter without synchronisation. Fixed with per-thread local accumulation
and a single write at thread exit, avoiding the coherence traffic an
atomic counter would add to the hot path.
