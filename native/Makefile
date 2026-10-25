CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -fPIC
AR ?= ar

TARGET_LIB = libvr_reactor_bridge.a
TEST_BIN = test_vr_reactor

.PHONY: all clean test

all: $(TARGET_LIB) $(TEST_BIN) libreactor_bridge.a test_reactor

$(TARGET_LIB): vr_reactor_epoll.o vr_ws_codec.o vr_sse_codec.o
	$(AR) rcs $@ $^

libreactor_bridge.a: $(TARGET_LIB)
	cp $(TARGET_LIB) $@

vr_reactor_epoll.o: vr_reactor_epoll.c vr_reactor_bridge.h
	$(CC) $(CFLAGS) -c vr_reactor_epoll.c -o $@

vr_ws_codec.o: vr_ws_codec.c vr_reactor_bridge.h
	$(CC) $(CFLAGS) -c vr_ws_codec.c -o $@

vr_sse_codec.o: vr_sse_codec.c vr_reactor_bridge.h
	$(CC) $(CFLAGS) -c vr_sse_codec.c -o $@

$(TEST_BIN): test_vr_reactor.c $(TARGET_LIB)
	$(CC) $(CFLAGS) test_vr_reactor.c -L. -lvr_reactor_bridge -o $@

test_reactor: $(TEST_BIN)
	cp $(TEST_BIN) $@

test: $(TEST_BIN)
	./$(TEST_BIN)

clean:
	rm -f *.o $(TARGET_LIB) $(TEST_BIN) libreactor_bridge.a test_reactor
