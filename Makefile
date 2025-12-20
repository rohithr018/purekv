CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -Iinclude
LDFLAGS := -lz

BUILD := build

TARGET := $(BUILD)/kv_engine
BENCH  := $(BUILD)/kv_bench

#source files
ENGINE_SRC := src/kv_engine.cpp \
              src/wal.cpp \
              src/segment.cpp

APP_SRC := main.cpp
BENCH_SRC := bench/bench.cpp

ENGINE_OBJ := $(patsubst %.cpp,$(BUILD)/%.o,$(ENGINE_SRC))
APP_OBJ    := $(patsubst %.cpp,$(BUILD)/%.o,$(APP_SRC))
BENCH_OBJ  := $(patsubst %.cpp,$(BUILD)/%.o,$(BENCH_SRC))

.PHONY: all run bench clean distclean

# all target
all: $(TARGET) $(BENCH)

# kv_engine build
$(TARGET): $(ENGINE_OBJ) $(APP_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# kv_bench build
$(BENCH): $(ENGINE_OBJ) $(BENCH_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# compilation rule
$(BUILD)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# helpers
run:
	./$(TARGET) $(ARGS)

bench:
	./$(BENCH) $(ARGS)

clean:
	rm -rf $(BUILD)

distclean:
	rm -rf $(BUILD) wal segments
