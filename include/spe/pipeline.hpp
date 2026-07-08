#pragma once
// pipeline.hpp — Multi-stage, event-driven pipeline (CV bullet 3).
//
// Topology:  generator -> [ring q1] -> transform stage -> [ring q2] -> aggregate stage
//
// Each arrow is a bounded SPSC ring; each stage is one thread. Because every
// queue is SPSC, the whole pipeline stays lock-free without needing MPMC
// machinery. Backpressure: when a downstream ring is full, the upstream
// stage SPINS-THEN-YIELDS rather than dropping events — i.e. pressure
// propagates backwards until the source slows down. That "bounded queues +
// blocking producer" scheme is the classic backpressure design; the
// alternative (drop + count) is one line away and discussed in the README.
//
// The transform stage is deliberately cheap (scale + clamp) so benchmarks
// measure the ENGINE's overhead, not an artificial workload.

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "spe/event.hpp"
#include "spe/spsc_ring.hpp"

namespace spe {

struct PipelineStats {
    uint64_t produced{0};
    uint64_t transformed{0};
    uint64_t aggregated{0};
    uint64_t windows_closed{0};
    // Per-stage stall counters: each is written by exactly ONE thread.
    // (A single shared counter here was a genuine data race caught by TSan —
    // producer and transformer both incremented it unsynchronised.)
    uint64_t producer_stalls{0};
    uint64_t transformer_stalls{0};
    uint64_t total_stalls() const { return producer_stalls + transformer_stalls; }
};

class Pipeline {
public:
    Pipeline(std::size_t ring_capacity, uint64_t window_width_ns, std::size_t batch = 64)
        : q1_(ring_capacity), q2_(ring_capacity),
          window_(window_width_ns), batch_(batch) {}

    // Run the pipeline over `total` synthetic events. Blocks until drained.
    PipelineStats run(uint64_t total) {
        PipelineStats stats{};
        std::atomic<bool> gen_done{false}, xform_done{false};

        std::thread producer([&] {
            std::vector<Event> chunk(batch_);
            uint64_t seq = 0, my_stalls = 0;
            // xorshift RNG — cheap, deterministic synthetic load
            uint64_t rng = 0x9E3779B97F4A7C15ull;
            while (seq < total) {
                const std::size_t n = std::min<uint64_t>(batch_, total - seq);
                for (std::size_t i = 0; i < n; ++i) {
                    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                    chunk[i] = Event{seq + i,
                                     /*ts_ns=*/ (seq + i) * 1000, // 1 event/us logical time
                                     /*key=*/  uint32_t(rng & 0xFF),
                                     /*value=*/ float(rng % 10000) / 100.0f};
                }
                std::size_t pushed = 0;
                while (pushed < n) {
                    const std::size_t k = q1_.push_batch(chunk.data() + pushed, n - pushed);
                    if (k == 0) { ++my_stalls; std::this_thread::yield(); }
                    pushed += k;
                }
                seq += n;
            }
            stats.produced = seq;
            stats.producer_stalls = my_stalls;   // single write after the hot loop
            gen_done.store(true, std::memory_order_release);
        });

        std::thread transformer([&] {
            std::vector<Event> in(batch_);
            uint64_t count = 0, my_stalls = 0;
            for (;;) {
                const std::size_t n = q1_.pop_batch(in.data(), batch_);
                if (n == 0) {
                    if (gen_done.load(std::memory_order_acquire)) {
                        // drain check: one more attempt in case of race
                        if (q1_.pop_batch(in.data(), batch_) == 0) break;
                    }
                    std::this_thread::yield();
                    continue;
                }
                for (std::size_t i = 0; i < n; ++i) {           // the "work"
                    in[i].value = in[i].value * 1.5f + 1.0f;
                    if (in[i].value > 200.0f) in[i].value = 200.0f;
                }
                std::size_t pushed = 0;
                while (pushed < n) {
                    const std::size_t k = q2_.push_batch(in.data() + pushed, n - pushed);
                    if (k == 0) { ++my_stalls; std::this_thread::yield(); }
                    pushed += k;
                }
                count += n;
            }
            stats.transformed = count;
            stats.transformer_stalls = my_stalls;   // single write after the hot loop
            xform_done.store(true, std::memory_order_release);
        });

        std::thread aggregator([&] {
            std::vector<Event> in(batch_);
            uint64_t count = 0, closed = 0;
            for (;;) {
                const std::size_t n = q2_.pop_batch(in.data(), batch_);
                if (n == 0) {
                    if (xform_done.load(std::memory_order_acquire)) {
                        if (q2_.pop_batch(in.data(), batch_) == 0) break;
                    }
                    std::this_thread::yield();
                    continue;
                }
                for (std::size_t i = 0; i < n; ++i)
                    if (window_.add(in[i])) ++closed;
                count += n;
            }
            stats.aggregated = count;
            stats.windows_closed = closed;
        });

        producer.join();
        transformer.join();
        aggregator.join();
        return stats;
    }

    const TumblingWindow& window() const { return window_; }

private:
    SpscRing<Event> q1_;
    SpscRing<Event> q2_;
    TumblingWindow window_;
    std::size_t batch_;
};

} // namespace spe
