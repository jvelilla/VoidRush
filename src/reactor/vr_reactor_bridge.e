note
	description: "Low-level FFI bridge to native VR reactor static library"

class
	VR_REACTOR_BRIDGE

inherit
	VR_REACTOR_CONSTANTS

feature -- Lifecycle

	c_reactor_create: POINTER
			-- Create a new native reactor instance.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_POINTER) vr_reactor_create();"
		ensure
			instance_free: class
		end

	c_reactor_destroy (r: POINTER)
			-- Destroy native reactor instance and release all internal file descriptors.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_destroy((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

feature -- Atomic Shutdown Control

	c_reactor_request_shutdown (r: POINTER)
			-- Set atomic shutdown flag and wake up blocked reactor thread.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_request_shutdown((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_is_shutting_down (r: POINTER): INTEGER
			-- Check if atomic shutdown flag has been raised (1 = true, 0 = false).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_is_shutting_down((const vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

feature -- Interest Management

	c_reactor_add (r: POINTER; fd: INTEGER; flags, generation: NATURAL_32): INTEGER
			-- Register file descriptor `fd` with `flags` and `generation` token.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_add((vr_reactor_t *) $r, (int) $fd, (uint32_t) $flags, (uint32_t) $generation);"
		ensure
			instance_free: class
		end

	c_reactor_mod (r: POINTER; fd: INTEGER; flags, generation: NATURAL_32): INTEGER
			-- Modify interest flags and generation for `fd` (thread-safe from worker processors).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_mod((vr_reactor_t *) $r, (int) $fd, (uint32_t) $flags, (uint32_t) $generation);"
		ensure
			instance_free: class
		end

	c_reactor_del (r: POINTER; fd: INTEGER): INTEGER
			-- Remove `fd` from reactor interest set.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_del((vr_reactor_t *) $r, (int) $fd);"
		ensure
			instance_free: class
		end

feature -- Poll & Wakeup

	c_reactor_wait (r, events: POINTER; max_events_to_poll, timeout_ms: INTEGER): INTEGER
			-- Blocking poll for ready events into pre-allocated `events` buffer.
			-- Marked as C blocking so other SCOOP processors and Eiffel GC are not suspended.
		external
			"C blocking use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_wait"
		ensure
			instance_free: class
		end

	c_reactor_wakeup (r: POINTER): INTEGER
			-- Wake up reactor blocked in `c_reactor_wait` via internal eventfd.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_wakeup((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_wake (r: POINTER)
			-- Wake up reactor thread (procedure wrapper discarding return code).
		local
			res: INTEGER
		do
			res := c_reactor_wakeup (r)
		ensure
			instance_free: class
		end

feature -- Socket Helpers

	c_listen_tcp (host: POINTER; port, backlog: INTEGER): INTEGER
			-- Create non-blocking TCP listening socket bound to `host` and `port`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_listen_tcp((const char *) $host, (int) $port, (int) $backlog);"
		ensure
			instance_free: class
		end

	c_accept_nonblocking (listen_fd: INTEGER): INTEGER
			-- Accept one connection with non-blocking, cloexec, and TCP_NODELAY enabled.
			-- Returns client socket fd, or Reactor_io_would_block (-1), or Reactor_io_error (-2).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_accept_nonblocking((int) $listen_fd);"
		ensure
			instance_free: class
		end

	c_close (fd: INTEGER): INTEGER
			-- Close file descriptor `fd`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_close((int) $fd);"
		ensure
			instance_free: class
		end

	c_set_nonblocking (fd: INTEGER): INTEGER
			-- Set non-blocking mode on socket `fd`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_set_nonblocking((int) $fd);"
		ensure
			instance_free: class
		end

	c_shutdown_wr (fd: INTEGER): INTEGER
			-- Half-close write side of TCP socket (sends FIN to peer).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_shutdown_wr((int) $fd);"
		ensure
			instance_free: class
		end

	c_connect_tcp (host: POINTER; port: INTEGER): INTEGER
			-- Create TCP socket and connect to `host` and `port`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_connect_tcp((const char *) $host, (int) $port);"
		ensure
			instance_free: class
		end

	c_send (fd: INTEGER; buf: POINTER; len: INTEGER): INTEGER
			-- Send `len` bytes from `buf` to socket `fd`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_send((int) $fd, (const char *) $buf, (int) $len);"
		ensure
			instance_free: class
		end

feature -- Non-blocking Data I/O (Phase 3)

	c_reactor_read (fd: INTEGER; buf: POINTER; len: INTEGER): INTEGER
			-- Read up to `len` bytes into `buf`. Returns bytes read (> 0),
			-- Reactor_io_eof (0), Reactor_io_would_block (-1), or Reactor_io_error (-2).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_read((int) $fd, (void *) $buf, (size_t) $len);"
		ensure
			instance_free: class
		end

	c_reactor_write (fd: INTEGER; buf: POINTER; len: INTEGER): INTEGER
			-- Write up to `len` bytes from `buf`. Returns bytes written (> 0),
			-- Reactor_io_would_block (-1), or Reactor_io_error (-2).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_write((int) $fd, (const void *) $buf, (size_t) $len);"
		ensure
			instance_free: class
		end

	c_reactor_now_ms: NATURAL_64
			-- Fast monotonic millisecond clock via CLOCK_MONOTONIC_COARSE.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_64) vr_reactor_now_ms();"
		ensure
			instance_free: class
		end

	c_sleep_ms (ms: INTEGER)
			-- Sleep for `ms` milliseconds.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_sleep_ms((int) $ms);"
		ensure
			instance_free: class
		end

	c_memmove (dest, src: POINTER; n: INTEGER)
			-- Shift memory region handling overlapping segments.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_memmove((void *) $dest, (const void *) $src, (size_t) $n);"
		ensure
			instance_free: class
		end

feature -- Edge-Triggered Drain & Discard

	c_reactor_drain_discard (fd: INTEGER): INTEGER
			-- Drain incoming socket bytes until EAGAIN.
			-- Returns Reactor_io_would_block (-1), Reactor_io_eof (0), or Reactor_io_error (-2).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_drain_discard((int) $fd);"
		ensure
			instance_free: class
		end

feature -- Atomic Metrics

	c_metric_inc_accept (r: POINTER)
			-- Increment total accepts counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_accept((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_open (r: POINTER)
			-- Increment active connections counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_open((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_dec_open (r: POINTER)
			-- Decrement active connections counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_dec_open((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_events (r: POINTER; count: NATURAL_64)
			-- Increment total events processed counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_events((vr_reactor_t *) $r, (uint64_t) $count);"
		ensure
			instance_free: class
		end

	c_metric_inc_stale (r: POINTER)
			-- Increment stale events dropped counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_stale((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_add_bytes_in (r: POINTER; bytes: NATURAL_64)
			-- Add bytes to total inbound counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_add_bytes_in((vr_reactor_t *) $r, (uint64_t) $bytes);"
		ensure
			instance_free: class
		end

	c_metric_add_bytes_out (r: POINTER; bytes: NATURAL_64)
			-- Add bytes to total outbound counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_add_bytes_out((vr_reactor_t *) $r, (uint64_t) $bytes);"
		ensure
			instance_free: class
		end

	c_metric_inc_idle_timeouts (r: POINTER)
			-- Increment total idle/slowloris timeouts counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_idle_timeouts((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_ws_upgrades (r: POINTER)
			-- Increment total WebSocket upgrades counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_ws_upgrades((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_ws_frames_in (r: POINTER)
			-- Increment total inbound WebSocket frames counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_ws_frames_in((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_ws_frames_out (r: POINTER)
			-- Increment total outbound WebSocket frames counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_ws_frames_out((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_ws_dropped (r: POINTER)
			-- Increment total frames dropped due to subscriber buffer backpressure.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_ws_dropped((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_sse_streams (r: POINTER)
			-- Increment total active SSE streams counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_sse_streams((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_sse_events_out (r: POINTER)
			-- Increment total outbound SSE events counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_sse_events_out((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_sse_heartbeats (r: POINTER)
			-- Increment total SSE comment heartbeats counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_sse_heartbeats((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_sse_dropped (r: POINTER)
			-- Increment total SSE events dropped due to backpressure.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_sse_dropped((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_set_tcp_user_timeout (fd: INTEGER; timeout_ms: NATURAL_32): INTEGER
			-- Configure TCP_USER_TIMEOUT socket option.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_set_tcp_user_timeout((int) $fd, (unsigned int) $timeout_ms);"
		ensure
			instance_free: class
		end

	c_reactor_get_metrics (r, out_buf: POINTER)
			-- Snapshot metrics into `out_buf` (must be at least 120 bytes).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_get_metrics((const vr_reactor_t *) $r, (vr_reactor_metrics_t *) $out_buf);"
		ensure
			instance_free: class
		end

feature -- Phase 5 WebSocket Primitives

	c_ws_compute_accept_key (client_key: POINTER; key_len: INTEGER; out_accept: POINTER): INTEGER
			-- Compute RFC 6455 Sec-WebSocket-Accept token into `out_accept` (min 29 bytes).
			-- Returns 28 on success, -1 on invalid input.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_ws_compute_accept_key((const char *) $client_key, (size_t) $key_len, (char *) $out_accept);"
		ensure
			instance_free: class
		end

	c_ws_unmask (payload: POINTER; len: INTEGER; mask_key: POINTER)
			-- Fast 64-bit word-aligned in-place XOR unmasking.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_ws_unmask((uint8_t *) $payload, (size_t) $len, (const uint8_t *) $mask_key);"
		ensure
			instance_free: class
		end

	c_ws_encode_frame_header (fin_and_opcode: NATURAL_8; payload_len: INTEGER; out_hdr: POINTER): INTEGER
			-- Encode unmasked server frame header (2, 4, or 10 bytes).
			-- Returns header length in bytes.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_ws_encode_frame_header((uint8_t) $fin_and_opcode, (size_t) $payload_len, (uint8_t *) $out_hdr);"
		ensure
			instance_free: class
		end

	c_ws_msg_create (opcode: INTEGER; payload: POINTER; len, initial_refs: INTEGER): POINTER
			-- Allocate immutable reference-counted pre-framed WebSocket message buffer.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_POINTER) vr_ws_msg_create((uint8_t) $opcode, (const void *) $payload, (size_t) $len, (int) $initial_refs);"
		ensure
			instance_free: class
		end

	c_ws_msg_data (msg: POINTER): POINTER
			-- Pointer to pre-framed raw bytes (header + payload).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_POINTER) vr_ws_msg_data((const vr_ws_msg_t *) $msg);"
		ensure
			instance_free: class
		end

	c_ws_msg_size (msg: POINTER): INTEGER
			-- Total byte size of pre-framed message.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_ws_msg_size((const vr_ws_msg_t *) $msg);"
		ensure
			instance_free: class
		end

	c_ws_msg_unref (msg: POINTER)
			-- Decrement atomic reference counter, freeing memory when count reaches 0.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_ws_msg_unref((vr_ws_msg_t *) $msg);"
		ensure
			instance_free: class
		end

	c_has_pub_prefix (ptr: POINTER; len: INTEGER): BOOLEAN
			-- Does memory slice begin with ASCII "PUB "?
		external
			"C inline"
		alias
			"return (EIF_BOOLEAN) ($len >= 4 && ((const char *)$ptr)[0] == 'P' && ((const char *)$ptr)[1] == 'U' && ((const char *)$ptr)[2] == 'B' && ((const char *)$ptr)[3] == ' ');"
		ensure
			instance_free: class
		end

feature -- Phase 6 Server-Sent Events (SSE) Primitives

	c_sse_format_event (id: POINTER; id_len: INTEGER; event: POINTER; event_len: INTEGER; data: POINTER; data_len: INTEGER; out_buf: POINTER; out_cap: INTEGER): INTEGER
			-- Format an SSE event into `out_buf` (up to `out_cap` bytes).
			-- Returns bytes written, or -1 if capacity is insufficient.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_sse_format_event((const char *) $id, (size_t) $id_len, (const char *) $event, (size_t) $event_len, (const char *) $data, (size_t) $data_len, (char *) $out_buf, (size_t) $out_cap);"
		ensure
			instance_free: class
		end

	c_sse_msg_create (id: POINTER; id_len: INTEGER; event: POINTER; event_len: INTEGER; data: POINTER; data_len: INTEGER; initial_refs: INTEGER): POINTER
			-- Allocate immutable reference-counted pre-framed SSE broadcast message buffer.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_POINTER) vr_sse_msg_create((const char *) $id, (size_t) $id_len, (const char *) $event, (size_t) $event_len, (const char *) $data, (size_t) $data_len, (int) $initial_refs);"
		ensure
			instance_free: class
		end

	c_has_pub_sse_prefix (ptr: POINTER; len: INTEGER): BOOLEAN
			-- Does memory slice begin with ASCII "PUB_SSE "?
		external
			"C inline"
		alias
			"return (EIF_BOOLEAN) ($len >= 8 && ((const char *)$ptr)[0] == 'P' && ((const char *)$ptr)[1] == 'U' && ((const char *)$ptr)[2] == 'B' && ((const char *)$ptr)[3] == '_' && ((const char *)$ptr)[4] == 'S' && ((const char *)$ptr)[5] == 'S' && ((const char *)$ptr)[6] == 'E' && ((const char *)$ptr)[7] == ' ');"
		ensure
			instance_free: class
		end

feature -- Phase 7 Supervision Heartbeats, Faults & Shard Purge

	c_reactor_beat_reactor (r: POINTER; now_ms: NATURAL_64)
			-- Update atomic liveness timestamp for reactor.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_beat_reactor((vr_reactor_t *) $r, (uint64_t) $now_ms);"
		ensure
			instance_free: class
		end

	c_reactor_beat_broker (r: POINTER; now_ms: NATURAL_64)
			-- Update atomic liveness timestamp for broker.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_beat_broker((vr_reactor_t *) $r, (uint64_t) $now_ms);"
		ensure
			instance_free: class
		end

	c_reactor_beat_worker (r: POINTER; worker_id: INTEGER; now_ms: NATURAL_64)
			-- Update atomic liveness timestamp for worker shard.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_beat_worker((vr_reactor_t *) $r, (int) $worker_id, (uint64_t) $now_ms);"
		ensure
			instance_free: class
		end

	c_reactor_get_reactor_beat (r: POINTER): NATURAL_64
			-- Query latest atomic reactor heartbeat timestamp without locking.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_64) vr_reactor_get_reactor_beat((const vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_get_broker_beat (r: POINTER): NATURAL_64
			-- Query latest atomic broker heartbeat timestamp without locking.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_64) vr_reactor_get_broker_beat((const vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_get_worker_beat (r: POINTER; worker_id: INTEGER): NATURAL_64
			-- Query latest atomic worker heartbeat timestamp without locking.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_64) vr_reactor_get_worker_beat((const vr_reactor_t *) $r, (int) $worker_id);"
		ensure
			instance_free: class
		end

	c_reactor_report_broker_fault (r: POINTER)
			-- Atomically signal broker fault in bitmask.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_report_broker_fault((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_report_worker_fault (r: POINTER; worker_id: INTEGER)
			-- Atomically signal worker fault in bitmask.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_report_worker_fault((vr_reactor_t *) $r, (int) $worker_id);"
		ensure
			instance_free: class
		end

	c_reactor_consume_fault_mask (r: POINTER): NATURAL_64
			-- Atomically exchange and clear child fault bitmask.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_64) vr_reactor_consume_fault_mask((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_set_fd_owner (r: POINTER; fd: INTEGER; worker_id: INTEGER_16)
			-- Record shard worker ownership for `fd`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_set_fd_owner((vr_reactor_t *) $r, (int) $fd, (int16_t) $worker_id);"
		ensure
			instance_free: class
		end

	c_reactor_get_fd_owner (r: POINTER; fd: INTEGER): INTEGER_16
			-- Query shard worker ownership for `fd`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER_16) vr_reactor_get_fd_owner((const vr_reactor_t *) $r, (int) $fd);"
		ensure
			instance_free: class
		end

	c_reactor_purge_worker_fds (r: POINTER; worker_id: INTEGER): INTEGER
			-- Purge and close all sockets owned by `worker_id` from epoll.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_purge_worker_fds((vr_reactor_t *) $r, (int) $worker_id);"
		ensure
			instance_free: class
		end

	c_metric_inc_restarts (r: POINTER)
			-- Increment supervisor child restarts counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_restarts((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_metric_inc_isolated_faults (r: POINTER)
			-- Increment connection isolated faults counter.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_metric_inc_isolated_faults((vr_reactor_t *) $r);"
		ensure
			instance_free: class
		end

	c_reactor_assign_worker (r: POINTER; fd: INTEGER; worker_count: INTEGER): INTEGER
			-- Assign socket `fd` to the worker with the lowest active connection count.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_assign_worker((vr_reactor_t *) $r, (int) $fd, (int) $worker_count);"
		ensure
			instance_free: class
		end

	c_reactor_route_fd (r: POINTER; fd: INTEGER; worker_count: INTEGER): INTEGER
			-- Resolve worker partition owning `fd` for event dispatch, or -1 if unowned.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_INTEGER) vr_reactor_route_fd((const vr_reactor_t *) $r, (int) $fd, (int) $worker_count);"
		ensure
			instance_free: class
		end

	c_reactor_release_worker_conn (r: POINTER; worker_id: INTEGER)
			-- Decrement the active connection count for `worker_id`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_release_worker_conn((vr_reactor_t *) $r, (int) $worker_id);"
		ensure
			instance_free: class
		end

	c_reactor_get_worker_conns (r: POINTER; worker_id: INTEGER): NATURAL_32
			-- Active connection count tracked on worker shard `worker_id`.
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"return (EIF_NATURAL_32) vr_reactor_get_worker_conns((const vr_reactor_t *) $r, (int) $worker_id);"
		ensure
			instance_free: class
		end

end
