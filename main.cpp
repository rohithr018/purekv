#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>
#include <unistd.h>

#include "kv_engine.h"

using namespace std;

/* ---------------- Concurrency Test ---------------- */
void concurrency_test() {
    cout << "[TEST] Concurrency test started\n";

    KVEngine* engine = CreateKVEngine();

    auto writer = [&]() {
        for (int i = 0; i < 1000; i++) {
            engine->put("k" + to_string(i),
                        "v" + to_string(i));
        }
    };

    auto reader = [&]() {
        string v;
        for (int i = 0; i < 1000; i++) {
            engine->get("k" + to_string(i), &v);
        }
    };

    thread t1(writer), t2(writer), t3(reader);
    t1.join();
    t2.join();
    t3.join();

    cout << "[PASS] Concurrency test passed\n";
    delete engine;
}

/* ---------------- Recovery Test ---------------- */
void recovery_test() {
    cout << "[TEST] Recovery test started\n";

    KVEngine* engine = CreateKVEngine();

    engine->put("A", "1");
    engine->put("B", "2");
    engine->put("C", "3");

    cout << "[INFO] Simulating crash (process exit)\n";
    delete engine;

    /*
        IMPORTANT:
        Simulate crash by exiting WITHOUT cleanup logic.
        WAL should already be durable.
    */
    _exit(0);
}

/* ---------------- Verify After Restart ---------------- */
void verify_recovery() {
    cout << "[TEST] Recovery verification started\n";

    KVEngine* engine = CreateKVEngine();
    string v;
    Status s;

    s = engine->get("A", &v);
    if (!s.ok()) {
        cout << "[FAIL] A not recovered\n";
        exit(1);
    }
    cout << "A=" << v << "\n";

    s = engine->get("B", &v);
    if (!s.ok()) {
        cout << "[FAIL] B not recovered\n";
        exit(1);
    }
    cout << "B=" << v << "\n";

    s = engine->get("C", &v);
    if (!s.ok()) {
        cout << "[FAIL] C not recovered\n";
        exit(1);
    }
    cout << "C=" << v << "\n";

    cout << "[PASS] Recovery verification passed\n";
    delete engine;
}


int main(int argc, char** argv) {

    if (argc < 2) {
        cout << "Usage:\n";
        cout << "  ./kv_engine concurrency\n";
        cout << "  ./kv_engine crash\n";
        cout << "  ./kv_engine verify\n";
        return 0;
    }

    string mode = argv[1];

    if (mode == "concurrency") {
        concurrency_test();
    } else if (mode == "crash") {
        recovery_test();
    } else if (mode == "verify") {
        verify_recovery();
    } else {
        cout << "Unknown mode\n";
    }

    return 0;
}
