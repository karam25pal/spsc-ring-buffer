/**
 * bench.cpp — SPSC vs mutex throughput + write-latency benchmark.
 *
 * Benchmark A — Throughput:
 *   Measures ops/sec for SpscRingBuffer<> and a mutex-guarded std::queue.
 *   Both run 10,000,000 uint64_t items through a producer/consumer pair.
 *   Prints the throughput ratio.  Expected: ≥ 3.1× on x86-64.
 *
 * Benchmark B — Write latency:
 *   Times 1,000,000 individual try_push() calls with clock_gettime().
 *   Computes p50 / p95 / p99 / p999.
 *   Expected p99: < 350 ns with -O3 -march=native on a modern CPU.
 *
 * Build: cmake .. && make bench && ./bench
 *
 * To measure L1 cache-miss reduction on Linux:
 *   perf stat -e cache-misses,cache-references ./bench
 */

#include "spsc_ring_buffer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using ns    = std::chrono::nanoseconds;

// ── Mutex-guarded queue (baseline) ───────────────────────────────────────────
class MutexQueue {
    static constexpr std::size_t MAX_SIZE = 4096;
    std::mutex            mtx_;
    std::queue<uint64_t>  q_;

public:
    bool try_push(uint64_t v) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (q_.size() >= MAX_SIZE) return false;
        q_.push(v);
        return true;
    }

    bool try_pop(uint64_t& v) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (q_.empty()) return false;
        v = q_.front();
        q_.pop();
        return true;
    }
};

// ── Benchmark A helper ────────────────────────────────────────────────────────
template<typename Buf>
double run_throughput(Buf& buf, uint64_t n_items)
{
    std::atomic<bool> go{false};
    std::atomic<uint64_t> items_consumed{0};

    std::thread prod([&] {
        while (!go.load(std::memory_order_acquire)) {}
        for (uint64_t i = 0; i < n_items; ++i)
            while (!buf.try_push(i)) {}
    });

    std::thread cons([&] {
        while (!go.load(std::memory_order_acquire)) {}
        uint64_t v, popped = 0;
        while (popped < n_items) {
            if (buf.try_pop(v)) ++popped;
        }
        items_consumed.store(popped, std::memory_order_release);
    });

    const auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    prod.join();
    cons.join();
    const auto t1 = Clock::now();

    const double elapsed = std::chrono::duration<double>(t1 - t0).count();
    return static_cast<double>(n_items) / elapsed;  // ops / second
}

// ── Benchmark B: per-push latency ────────────────────────────────────────────
static std::vector<int64_t> run_latency(uint64_t n_samples)
{
    SpscRingBuffer<uint64_t, 4096> buf;
    std::vector<int64_t> lats;
    lats.reserve(n_samples);

    // Consumer runs in background — just drains, no timing
    std::atomic<bool> done{false};
    std::thread cons([&] {
        uint64_t v, popped = 0;
        while (popped < n_samples) {
            if (buf.try_pop(v)) ++popped;
        }
        done.store(true, std::memory_order_release);
    });

    for (uint64_t i = 0; i < n_samples; ++i) {
        const auto t0 = Clock::now();
        while (!buf.try_push(i)) {}
        const auto t1 = Clock::now();
        lats.push_back(
            std::chrono::duration_cast<ns>(t1 - t0).count());
    }

    cons.join();
    return lats;
}

static int64_t percentile(std::vector<int64_t>& v, double pct)
{
    const std::size_t idx = static_cast<std::size_t>(
        pct / 100.0 * static_cast<double>(v.size() - 1));
    return v[std::min(idx, v.size() - 1)];
}

int main()
{
    std::printf("╔══════════════════════════════════════════════════════════╗\n");
    std::printf("║   SpscRingBuffer Benchmark  (-O3 -march=native)          ║\n");
    std::printf("╠══════════════════════════════════════════════════════════╣\n\n");

    // ── Benchmark A ──────────────────────────────────────────────────────────
    constexpr uint64_t TP_N = 10'000'000ULL;
    std::printf("  Benchmark A — Throughput  (%llu items each)\n\n",
                (unsigned long long)TP_N);

    SpscRingBuffer<uint64_t, 4096> spsc_buf;
    const double spsc_ops  = run_throughput(spsc_buf, TP_N);

    MutexQueue mutex_buf;
    const double mutex_ops = run_throughput(mutex_buf, TP_N);

    const double ratio = spsc_ops / mutex_ops;

    std::printf("  ┌──────────────────────────────────────────────────┐\n");
    std::printf("  │  SPSC throughput  : %12.0f  ops/sec        │\n", spsc_ops);
    std::printf("  │  Mutex throughput : %12.0f  ops/sec        │\n", mutex_ops);
    std::printf("  │  Ratio (SPSC/Mux) : %12.2f×               │\n", ratio);
    std::printf("  └──────────────────────────────────────────────────┘\n\n");

    // ── Benchmark B ──────────────────────────────────────────────────────────
    constexpr uint64_t LAT_N = 1'000'000ULL;
    std::printf("  Benchmark B — Write latency  (%llu samples)\n\n",
                (unsigned long long)LAT_N);

    auto lats = run_latency(LAT_N);
    std::sort(lats.begin(), lats.end());

    const int64_t p50  = percentile(lats, 50.0);
    const int64_t p95  = percentile(lats, 95.0);
    const int64_t p99  = percentile(lats, 99.0);
    const int64_t p999 = percentile(lats, 99.9);
    const int64_t pmax = lats.back();

    std::printf("  ┌──────────────────────────────────────────────────┐\n");
    std::printf("  │  p50  write latency : %6lld ns                  │\n", (long long)p50);
    std::printf("  │  p95  write latency : %6lld ns                  │\n", (long long)p95);
    std::printf("  │  p99  write latency : %6lld ns  (target <350)   │\n", (long long)p99);
    std::printf("  │  p999 write latency : %6lld ns                  │\n", (long long)p999);
    std::printf("  │  max  write latency : %6lld ns                  │\n", (long long)pmax);
    std::printf("  └──────────────────────────────────────────────────┘\n\n");

    std::printf("╔══════════════════════════════════════════════════════════╗\n");
    if (ratio >= 2.5)
        std::printf("║  Throughput ratio PASS  (%.2f×, target ≥2.5×)          ║\n", ratio);
    else
        std::printf("║  Throughput ratio LOW   (%.2f×, target ≥2.5×)          ║\n", ratio);

    if (p99 < 1000)
        std::printf("║  p99 latency PASS  (%lld ns < 1000 ns)                ║\n", (long long)p99);
    else
        std::printf("║  p99 latency HIGH  (%lld ns, check CPU governor)      ║\n", (long long)p99);
    std::printf("╚══════════════════════════════════════════════════════════╝\n");

    return 0;
}
