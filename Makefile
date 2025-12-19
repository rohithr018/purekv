CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -Iinclude 
LDFLAGS := -lz

BUILD := build
TARGET := $(BUILD)/kv_engine

SRC := main.cpp \
       src/kv_engine.cpp \
       src/wal.cpp

OBJ := $(patsubst %.cpp,$(BUILD)/%.o,$(SRC))

.PHONY: all run clean distclean

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

run: all
	./$(TARGET) $(ARGS)

clean:
	rm -rf $(BUILD)

distclean: clean
	rm -rf $(BUILD) wal
