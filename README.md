# Stream Processing Engine

A lock-free, multi-stage stream processing engine in C++20. Events flow
through bounded single-producer/single-consumer (SPSC) rings between
pipeline stages (generate → transform → windowed aggregate), with
backpressure propagating upstream when any stage falls behind.

## Layout

```
include/spe/spsc_ring.hpp    SPSC lock-free ring buffer:
                              - SpscRingNaive: per-event atomic ops (baseline)
                              - SpscRing: index caching + batched publication
include/spe/mutex_queue.hpp  Mutex-guarded bounded queue (fair comparison baseline)
include/spe/event.hpp        32-byte POD event + tumbling-window aggregation
include/spe/pipeline.hpp     3-stage pipeline with backpressure
bench/bench_main.cpp         Benchmark harness (throughput ladder + latency percentiles)
tests/tests.cpp              Correctness tests (run under TSan and ASan)
```

## Build & run

```bash
mkdir build && cd build
cmake .. && make
./tests               # correctness
./bench 20000000      # full benchmark, 20M events
```

Sanitizer runs (backs the TSan/ASan validation claim):

```bash
mkdir build-tsan && cd build-tsan && cmake .. -DSANITIZE=thread && make tests && ./tests
mkdir build-asan && cd build-asan && cmake .. -DSANITIZE=address && make tests && ./tests
```

## Where each CV figure comes from

| Slot | Meaning | Printed by bench as |
|------|---------|---------------------|
| [X]  | pipeline throughput, M events/sec | `3-stage pipeline` |
| [N]  | cores used | `cores available` |
| [Z]  | batched lock-free ÷ mutex throughput | `(…x vs mutex)  [Z]` |
| [A]  | batched vs naive lock-free gain | `batching gain over naive [A]` |
| [B/C/D] | p50 / p99 / p99.9 end-to-end latency | `end-to-end latency` block |

**Measure on your own hardware.** Container/VM numbers are not representative
(shared cores, no `-march=native` guarantees, noisy neighbours). For clean
numbers: close other applications, run on mains power, and consider pinning
threads (`taskset -c 0-3 ./bench`). Report the median of the printed runs.

## Benchmark methodology (the part interviewers probe)

- **Warmup pass** before every measured configuration (cold caches, page
  faults, and branch-predictor training would otherwise pollute run 1).
- **Median of 5 repetitions** for throughput — robust to interference,
  unlike best-of (cherry-picking) or mean (skewed by one bad run).
- **Latency measured separately from throughput**, sampling 1 event in 64.
  Timestamping every event would perturb the measurement (observer effect).
- **Percentiles, not means**, because latency is heavy-tailed and the tail
  is what matters: p50/p99/p99.9 are ranked positions in the sorted sample.
- **Fair baseline**: the mutex queue shares the ring storage, mask indexing,
  event type, and thread structure — only the synchronisation differs, so
  the [Z]× is attributable to the lock-free design and nothing else.

## Design decisions you should be able to defend

1. **Why SPSC (not MPMC)?** Each index has exactly one writer, so plain
   load/store with acquire/release suffices — no CAS loops, no ABA problem.
   Multi-producer needs are met by composing SPSC rings (one per producer),
   which is also how real systems often do it.
2. **Why acquire/release, not seq_cst?** We need exactly one guarantee: slot
   writes happen-before the index publish. Acquire/release provides it;
   seq_cst additionally imposes a global order across unrelated atomics,
   costing more (especially on ARM) for a property we don't use.
3. **Why does batching help?** Cross-core atomic ops force cache-line
   ownership transfers (MESI traffic). Publishing once per 64 events instead
   of per event cuts that traffic ~64×; index caching removes most loads of
   the other side's index. Synchronisation cost is amortised across a batch.
4. **Why cache-line-align head and tail?** They're written by different
   cores. On one line, every write by one core invalidates the other core's
   copy — false sharing — even though the variables are logically independent.
5. **Backpressure choice:** bounded rings + spinning/yielding producer means
   overload slows the source instead of dropping data (loss-less, at the
   cost of source latency). The drop-and-count alternative suits cases where
   freshness beats completeness; it's a one-line change in the push loop.
6. **A real bug TSan caught during development:** both producer and
   transformer originally incremented one shared `backpressure_stalls`
   counter — an unsynchronised same-address write, i.e. a data race. Fixed
   by per-thread local accumulation with a single write at thread exit
   (better than an atomic, which would add hot-path coherence traffic for
   a statistic). This is a good story about why sanitizers exist.
