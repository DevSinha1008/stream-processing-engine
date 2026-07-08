// tests.cpp — Correctness tests. Run under TSan/ASan via the sanitize build
// (see CMakeLists) to back the "validated under TSan/ASan" CV claim.
//
// The key SPSC test: producer pushes 0..N-1, consumer must receive exactly
// 0..N-1 in order with none lost or duplicated, across both APIs, while the
// threads genuinely race. Ordering violations from wrong memory_order would
// show up here (and TSan would flag the data race).

#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

#include "spe/event.hpp"
#include "spe/mutex_queue.hpp"
#include "spe/pipeline.hpp"
#include "spe/spsc_ring.hpp"

template <typename Q>
static void spsc_order_test(Q& q, uint64_t total, bool batch_api) {
    std::thread prod([&] {
        std::vector<spe::Event> c(64);
        uint64_t seq = 0;
        while (seq < total) {
            const std::size_t n = std::min<uint64_t>(64, total - seq);
            for (std::size_t i = 0; i < n; ++i)
                c[i] = spe::Event{seq + i, 0, 0, float(seq + i)};
            if constexpr (requires { q.push_batch(c.data(), n); }) {
                if (batch_api) {
                    std::size_t p = 0;
                    while (p < n) {
                        std::size_t k = q.push_batch(c.data() + p, n - p);
                        if (!k) std::this_thread::yield();
                        p += k;
                    }
                    seq += n; continue;
                }
            }
            for (std::size_t i = 0; i < n; ++i)
                while (!q.try_push(c[i])) std::this_thread::yield();
            seq += n;
        }
    });

    uint64_t expect = 0;
    std::vector<spe::Event> c(64);
    spe::Event e;
    while (expect < total) {
        if constexpr (requires { q.pop_batch(c.data(), std::size_t(64)); }) {
            if (batch_api) {
                std::size_t k = q.pop_batch(c.data(), 64);
                if (!k) { std::this_thread::yield(); continue; }
                for (std::size_t i = 0; i < k; ++i) {
                    assert(c[i].seq == expect && "out of order / lost event");
                    ++expect;
                }
                continue;
            }
        }
        if (q.try_pop(e)) {
            assert(e.seq == expect && "out of order / lost event");
            ++expect;
        } else std::this_thread::yield();
    }
    prod.join();
}

int main() {
    constexpr uint64_t N = 2'000'000;

    { spe::SpscRingNaive<spe::Event> q(1024); spsc_order_test(q, N, false);
      std::puts("PASS spsc naive per-event ordering"); }

    { spe::SpscRing<spe::Event> q(1024); spsc_order_test(q, N, false);
      std::puts("PASS spsc cached per-event ordering"); }

    { spe::SpscRing<spe::Event> q(1024); spsc_order_test(q, N, true);
      std::puts("PASS spsc batched ordering"); }

    { spe::MutexQueue<spe::Event> q(1024); spsc_order_test(q, N / 4, false);
      std::puts("PASS mutex queue ordering"); }

    // full-vs-empty edge: capacity-2 ring must reject a 3rd push, accept after pop
    { spe::SpscRing<int> q(2);
      assert(q.try_push(1) && q.try_push(2) && !q.try_push(3));
      int v; assert(q.try_pop(v) && v == 1);
      assert(q.try_push(3));
      std::puts("PASS full/empty edge cases"); }

    // windowing: 1ms windows over 1-event-per-us stream => 1000 events/window
    { spe::TumblingWindow w(1'000'000);
      uint64_t closed = 0;
      for (uint64_t i = 0; i < 5000; ++i)
          if (w.add(spe::Event{i, i * 1000, 0, 1.0f})) ++closed;
      assert(closed == 4 && "expected 4 closed 1ms windows over 5ms");
      assert(w.last_closed().count == 1000);
      std::puts("PASS tumbling window counts"); }

    // pipeline conservation: every produced event must be aggregated
    { spe::Pipeline pl(1 << 12, 1'000'000, 64);
      auto st = pl.run(1'000'000);
      assert(st.produced == 1'000'000 && st.aggregated == 1'000'000);
      std::puts("PASS pipeline conserves events"); }

    std::puts("ALL TESTS PASSED");
    return 0;
}
