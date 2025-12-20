#include "kv_engine.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>

using namespace std;
using Clock = chrono::high_resolution_clock;

long long elapsed_ms(Clock::time_point s, Clock::time_point e) {
    return chrono::duration_cast<chrono::milliseconds>(e - s).count();
}

// put benchmark
void bench_put() {
    cout << "[BENCH] PUT throughput\n";

    KVEngine* e = CreateKVEngine();
    const int N = 100000;

    auto start = Clock::now();
    for (int i = 0; i < N; i++) {
        e->put("k" + to_string(i), "v" + to_string(i));
    }
    auto end = Clock::now();

    long long ms = elapsed_ms(start, end);
    double ops = (double)N / (ms / 1000.0);

    cout << "Ops      : " << N << "\n";
    cout << "Time(ms) : " << ms << "\n";
    cout << "Ops/sec  : " << (long long)ops << "\n";

    delete e;
}

// get benchmark
void bench_get() {
    cout << "[BENCH] GET throughput\n";

    KVEngine* e = CreateKVEngine();
    const int N = 100000;

    for (int i = 0; i < N; i++) {
        e->put("k" + to_string(i), "v" + to_string(i));
    }

    string v;
    auto start = Clock::now();
    for (int i = 0; i < N; i++) {
        e->get("k" + to_string(i), &v);
    }
    auto end = Clock::now();

    long long ms = elapsed_ms(start, end);
    double ops = (double)N / (ms / 1000.0);

    cout << "Ops/sec  : " << (long long)ops << "\n";
    delete e;
}

// concurrent get benchmark
void bench_concurrent_get() {
    cout << "[BENCH] Concurrent GET throughput\n";

    KVEngine* e = CreateKVEngine();
    const int N = 100000;
    const int THREADS = 100;

    for (int i = 0; i < N; i++) {
        e->put("k" + to_string(i), "v" + to_string(i));
    }

    auto reader = [&]() {
        string v;
        for (int i = 0; i < N; i++) {
            e->get("k" + to_string(i), &v);
        }
    };

    vector<thread> ts;
    auto start = Clock::now();
    for (int i = 0; i < THREADS; i++) {
        ts.emplace_back(reader);
    }
    for (auto& t : ts) t.join();
    auto end = Clock::now();

    long long ops = (long long)N * THREADS;
    long long ms = elapsed_ms(start, end);

    cout << "Threads  : " << THREADS << "\n";
    cout << "Ops/sec  : " << (long long)(ops / (ms / 1000.0)) << "\n";

    delete e;
}


int main(int argc, char** argv) {
    if (argc < 2) {
        cout << "Usage:\n";
        cout << "  ./kv_bench put\n";
        cout << "  ./kv_bench get\n";
        cout << "  ./kv_bench concurrent\n";
        return 0;
    }

    string mode = argv[1];

    if (mode == "put") bench_put();
    else if (mode == "get") bench_get();
    else if (mode == "concurrent") bench_concurrent_get();
    else cout << "Unknown benchmark\n";

    return 0;
}
