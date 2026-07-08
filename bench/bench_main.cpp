// bench_main.cpp — Produces every figure slot on the CV.
//
// Methodology notes (interviewers ask about METHOD before numbers):
//
//  * Warmup: each benchmark does an untimed warmup pass so the first
//    measured run isn't paying for cold caches, page faults, or branch
//    predictor training.
//  * Repetitions: each config runs R times; we report the MEDIAN run for
//    throughput (robust to a noisy neighbour) rather than best or mean.
//  * Latency is measured in a SEPARATE run from throughput, and only a
//    1-in-64 sample of events is timestamped. Timestamping every event
//    with clock_gettime would perturb the very thing being measured
//    (observer effect) and throttle throughput.
//  * Percentiles come from sorting the sampled durations: p50/p99/p99.9
//    are the values at those rank positions. We report percentiles, not
//    means, because latency distributions are heavy-tailed and the tail
//    is what matters in real systems.
//  * The [Z]x comparison keeps EVERYTHING identical except the queue type:
//    same event struct, same counts, same threads, same capacity.
//
// Figures produced:
//   [X]  events/sec of the full pipeline (optimised rings, batched)
//   [N]  hardware threads used
//   [Z]  optimised-ring throughput / mutex-queue throughput
//   [A]  % gain of batched/cached ring over naive per-event-atomic ring
//   [B/C/D]  p50 / p99 / p99.9 end-to-end pipeline latency, nanoseconds

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "spe/event.hpp"
#include "spe/mutex_queue.hpp"
#include "spe/pipeline.hpp"
#include "spe/spsc_ring.hpp"

using Clock = std::chrono::steady_clock;
static double now_s() {
    return std::chrono::duration<double>(Clock::now().time_since_epoch()).count();
}
static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Single-queue producer->consumer throughput for a queue type Q.
// `use_batch` selects the batched API when the queue supports it.
// ---------------------------------------------------------------------------
template <typename Q>
static double queue_throughput(Q& q, uint64_t total, bool use_batch, std::size_t batch) {
    const double t0 = now_s();
    std::thread prod([&] {
        std::vector<spe::Event> chunk(batch);
        uint64_t seq = 0;
        while (seq < total) {
            const std::size_t n = std::min<uint64_t>(batch, total - seq);
            for (std::size_t i = 0; i < n; ++i)
                chunk[i] = spe::Event{seq + i, (seq + i) * 1000, 0, 1.0f};
            if constexpr (requires { q.push_batch(chunk.data(), n); }) {
                if (use_batch) {
                    std::size_t pushed = 0;
                    while (pushed < n) {
                        std::size_t k = q.push_batch(chunk.data() + pushed, n - pushed);
                        if (!k) std::this_thread::yield();
                        pushed += k;
                    }
                    seq += n;
                    continue;
                }
            }
            for (std::size_t i = 0; i < n; ++i)
                while (!q.try_push(chunk[i])) std::this_thread::yield();
            seq += n;
        }
    });
    std::thread cons([&] {
        spe::Event e;
        std::vector<spe::Event> chunk(batch);
        uint64_t got = 0;
        while (got < total) {
            if constexpr (requires { q.pop_batch(chunk.data(), batch); }) {
                if (use_batch) {
                    std::size_t k = q.pop_batch(chunk.data(), batch);
                    if (!k) { std::this_thread::yield(); continue; }
                    got += k;
                    continue;
                }
            }
            if (q.try_pop(e)) ++got; else std::this_thread::yield();
        }
    });
    prod.join(); cons.join();
    const double dt = now_s() - t0;
    return double(total) / dt; // events/sec
}

template <typename MakeQ>
static double median_throughput(MakeQ make, uint64_t total, bool use_batch,
                                std::size_t batch, int reps) {
    { auto q = make(); queue_throughput(*q, total / 4, use_batch, batch); } // warmup
    std::vector<double> runs;
    for (int r = 0; r < reps; ++r) {
        auto q = make();
        runs.push_back(queue_throughput(*q, total, use_batch, batch));
    }
    std::sort(runs.begin(), runs.end());
    return runs[runs.size() / 2];
}

// ---------------------------------------------------------------------------
// End-to-end pipeline latency: sampled timestamps through a 2-ring pipeline.
// Events carry a REAL send timestamp in ts_ns for 1-in-`stride` events;
// the final stage records (arrival - send). Separate from throughput runs.
// ---------------------------------------------------------------------------
static void pipeline_latency(uint64_t total, std::size_t cap, std::size_t batch,
                             uint64_t stride, std::vector<uint64_t>& out_ns) {
    spe::SpscRing<spe::Event> q1(cap), q2(cap);
    std::atomic<bool> d1{false}, d2{false};
    out_ns.clear();
    out_ns.reserve(total / stride + 1);

    std::thread p([&] {
        std::vector<spe::Event> c(batch);
        uint64_t seq = 0;
        while (seq < total) {
            const std::size_t n = std::min<uint64_t>(batch, total - seq);
            for (std::size_t i = 0; i < n; ++i) {
                const uint64_t s = seq + i;
                c[i] = spe::Event{s, (s % stride == 0) ? now_ns() : 0, 0, 1.0f};
            }
            std::size_t pushed = 0;
            while (pushed < n) {
                std::size_t k = q1.push_batch(c.data() + pushed, n - pushed);
                if (!k) std::this_thread::yield();
                pushed += k;
            }
            seq += n;
        }
        d1.store(true, std::memory_order_release);
    });
    std::thread x([&] {
        std::vector<spe::Event> c(batch);
        for (;;) {
            std::size_t n = q1.pop_batch(c.data(), batch);
            if (!n) {
                if (d1.load(std::memory_order_acquire) && !q1.pop_batch(c.data(), batch)) break;
                std::this_thread::yield(); continue;
            }
            for (std::size_t i = 0; i < n; ++i) c[i].value *= 1.5f;
            std::size_t pushed = 0;
            while (pushed < n) {
                std::size_t k = q2.push_batch(c.data() + pushed, n - pushed);
                if (!k) std::this_thread::yield();
                pushed += k;
            }
        }
        d2.store(true, std::memory_order_release);
    });
    std::thread s([&] {
        std::vector<spe::Event> c(batch);
        for (;;) {
            std::size_t n = q2.pop_batch(c.data(), batch);
            if (!n) {
                if (d2.load(std::memory_order_acquire) && !q2.pop_batch(c.data(), batch)) break;
                std::this_thread::yield(); continue;
            }
            const uint64_t arrive = now_ns();
            for (std::size_t i = 0; i < n; ++i)
                if (c[i].ts_ns) out_ns.push_back(arrive - c[i].ts_ns);
        }
    });
    p.join(); x.join(); s.join();
}

static uint64_t pct(std::vector<uint64_t>& v, double p) {
    if (v.empty()) return 0;
    const std::size_t idx = std::size_t(p * double(v.size() - 1));
    std::nth_element(v.begin(), v.begin() + idx, v.end());
    return v[idx];
}

int main(int argc, char** argv) {
    const uint64_t total  = (argc > 1) ? std::stoull(argv[1]) : 20'000'000ull;
    const std::size_t cap = 1 << 14;   // 16384-slot rings
    const std::size_t batch = 64;
    const int reps = 5;

    std::printf("cores available [N]: %u\n\n", std::thread::hardware_concurrency());

    // --- throughput ladder: mutex -> naive lock-free -> batched lock-free ---
    const double mtx = median_throughput(
        [&]{ return std::make_unique<spe::MutexQueue<spe::Event>>(cap); },
        total, /*batch=*/false, batch, reps);
    std::printf("mutex baseline      : %8.2f M ev/s\n", mtx / 1e6);

    const double naive = median_throughput(
        [&]{ return std::make_unique<spe::SpscRingNaive<spe::Event>>(cap); },
        total, /*batch=*/false, batch, reps);
    std::printf("lock-free (naive)   : %8.2f M ev/s   (%.1fx vs mutex)\n",
                naive / 1e6, naive / mtx);

    const double batched = median_throughput(
        [&]{ return std::make_unique<spe::SpscRing<spe::Event>>(cap); },
        total, /*batch=*/true, batch, reps);
    std::printf("lock-free (batched) : %8.2f M ev/s   (%.1fx vs mutex)  [Z]\n",
                batched / 1e6, batched / mtx);
    std::printf("batching gain over naive [A]: +%.1f%%\n\n",
                100.0 * (batched - naive) / naive);

    // --- full pipeline throughput [X] ---
    {
        spe::Pipeline pl(cap, /*window=*/1'000'000 /*1ms logical*/, batch);
        pl.run(total / 4); // warmup
    }
    std::vector<double> pruns;
    for (int r = 0; r < reps; ++r) {
        spe::Pipeline pl(cap, 1'000'000, batch);
        const double t0 = now_s();
        auto st = pl.run(total);
        const double dt = now_s() - t0;
        pruns.push_back(double(st.aggregated) / dt);
    }
    std::sort(pruns.begin(), pruns.end());
    std::printf("3-stage pipeline    : %8.2f M ev/s  [X]\n\n", pruns[reps/2] / 1e6);

    // --- latency percentiles [B]/[C]/[D] (separate run, sampled 1-in-64) ---
    std::vector<uint64_t> lat;
    pipeline_latency(total / 2, cap, batch, /*stride=*/64, lat); // warmup
    pipeline_latency(total, cap, batch, 64, lat);
    std::printf("end-to-end latency (n=%zu samples):\n", lat.size());
    std::printf("  p50   [B]: %6llu ns\n", (unsigned long long)pct(lat, 0.50));
    std::printf("  p99   [C]: %6llu ns\n", (unsigned long long)pct(lat, 0.99));
    std::printf("  p99.9 [D]: %6llu ns\n", (unsigned long long)pct(lat, 0.999));
    return 0;
}
