#pragma once
// event.hpp — The event flowing through the engine, plus windowed aggregation.
//
// Event is deliberately a small trivially-copyable POD: 32 bytes, half a
// cache line. Trivially-copyable matters because the ring buffer moves them
// by assignment in the hot path; no allocation ever happens per event.
//
// TumblingWindow implements windowed aggregation (CV bullet 3): rolling
// count / mean / min / max over fixed time windows keyed off the event
// timestamp. Tumbling = non-overlapping consecutive windows (the simplest
// correct windowing scheme; sliding windows are a natural extension).

#include <cstdint>
#include <limits>
#include <vector>

namespace spe {

struct Event {
    uint64_t seq;        // sequence number (assigned by the generator)
    uint64_t ts_ns;      // event timestamp, nanoseconds
    uint32_t key;        // partition/instrument/sensor id — domain-neutral
    float    value;      // the measurement
    uint32_t _pad{0};    // keep size a clean 32 bytes
};
static_assert(sizeof(Event) == 32);
static_assert(std::is_trivially_copyable_v<Event>);

struct WindowStats {
    uint64_t window_start_ns{0};
    uint64_t count{0};
    double   sum{0};
    float    min{std::numeric_limits<float>::max()};
    float    max{std::numeric_limits<float>::lowest()};
    double mean() const { return count ? sum / double(count) : 0.0; }
};

class TumblingWindow {
public:
    explicit TumblingWindow(uint64_t width_ns) : width_ns_(width_ns) {}

    // Feed one event. Returns true if a window just CLOSED (its stats are
    // then available via last_closed()) — i.e. this event's timestamp fell
    // past the current window boundary.
    bool add(const Event& e) {
        const uint64_t wstart = e.ts_ns - (e.ts_ns % width_ns_);
        bool closed = false;
        if (wstart != cur_.window_start_ns && cur_.count > 0) {
            last_ = cur_;
            closed = true;
            cur_ = WindowStats{};
        }
        cur_.window_start_ns = wstart;
        cur_.count += 1;
        cur_.sum   += e.value;
        if (e.value < cur_.min) cur_.min = e.value;
        if (e.value > cur_.max) cur_.max = e.value;
        return closed;
    }

    const WindowStats& last_closed() const { return last_; }
    const WindowStats& current() const { return cur_; }

private:
    uint64_t width_ns_;
    WindowStats cur_{};
    WindowStats last_{};
};

} // namespace spe
