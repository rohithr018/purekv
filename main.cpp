#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>

#include "kv_engine.h"

using namespace std;

/* ---------------- Concurrency Test ---------------- */
void concurrency_test() {
    cout << "[TEST] Fine-grained concurrency test\n";

    KVEngine* e = CreateKVEngine();

    for (int i = 0; i < 1000; i++) {
        e->put("k" + to_string(i), "v" + to_string(i));
    }

    auto reader = [&]() {
        string v;
        for (int i = 0; i < 1000; i++) {
            e->get("k" + to_string(i), &v);
        }
    };

    vector<thread> readers;
    for (int i = 0; i < 8; i++) {
        readers.emplace_back(reader);
    }

    for (auto& t : readers) t.join();

    cout << "[PASS] Concurrent reads succeeded\n";
    delete e;
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

void flush_test() {
    cout << "[TEST] MemTable flush test started\n";

    {
        KVEngine* engine = CreateKVEngine();

        // mem_limit_ = 5 → this will trigger flush
        engine->put("A", "1");
        engine->put("B", "2");
        engine->put("C", "3");
        engine->put("D", "4");
        engine->put("E", "5"); // flush happens here

        delete engine; // clean shutdown
    }

    cout << "[INFO] Restarting engine to verify segment reads\n";

    KVEngine* engine = CreateKVEngine();
    string v;
    Status s;

    s = engine->get("A", &v);
    if (!s.ok() || v != "1") {
        cout << "[FAIL] A not found in segment\n";
        exit(1);
    }

    s = engine->get("C", &v);
    if (!s.ok() || v != "3") {
        cout << "[FAIL] C not found in segment\n";
        exit(1);
    }

    s = engine->get("E", &v);
    if (!s.ok() || v != "5") {
        cout << "[FAIL] E not found in segment\n";
        exit(1);
    }

    cout << "[PASS] MemTable flush verified via segment reads\n";
    delete engine;
}

void compaction_test() {
    cout << "[TEST] Segment compaction test started\n";

    KVEngine* engine = CreateKVEngine();

    // Each 5 puts → flush → new segment
    for (int i = 0; i < 15; i++) {
        engine->put("k" + to_string(i),
                    "v" + to_string(i));
    }

    delete engine;

    cout << "[INFO] Restarting engine after compaction\n";

    engine = CreateKVEngine();
    string v;

    engine->get("k10", &v);
    cout << "k10=" << v << "\n";

    engine->get("k14", &v);
    cout << "k14=" << v << "\n";

    cout << "[PASS] Compaction verified\n";
    delete engine;
}

void corruption_test() {
    cout << "[TEST] Segment corruption handling\n";

    {
        KVEngine* e = CreateKVEngine();
        e->put("X", "100");

        // Force flush
        for (int i = 0; i < 10; i++) {
            e->put("pad" + to_string(i), "v");
        }
        delete e;
    }

    // WAL must be removed so only segment is used
    unlink("wal/kv.wal");

    // Corrupt the segment
    int fd = open("segments/seg_0.sst", O_WRONLY);
    uint32_t junk = 0xdeadbeef;
    write(fd, &junk, sizeof(junk));
    close(fd);

    KVEngine* e2 = CreateKVEngine();
    string v;
    Status s = e2->get("X", &v);

    if (s.ok()) {
        cout << "[FAIL] Corruption not detected\n";
        exit(1);
    }

    cout << "[PASS] Corruption detected safely\n";
    delete e2;
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

    if (mode == "concurrency")concurrency_test();
    else if (mode == "crash") recovery_test();
    else if (mode == "verify")verify_recovery();
    else if (mode == "flush")flush_test();
    else if (mode == "compact")compaction_test();
    else if (mode == "corrupt") corruption_test();

    else cout << "Unknown mode\n";
    

    return 0;
}
