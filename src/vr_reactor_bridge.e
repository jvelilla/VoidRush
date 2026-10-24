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

	c_reactor_get_metrics (r, out_buf: POINTER)
			-- Snapshot metrics into `out_buf` (must be at least 32 bytes).
		external
			"C inline use %"vr_reactor_bridge.h%""
		alias
			"vr_reactor_get_metrics((const vr_reactor_t *) $r, (vr_reactor_metrics_t *) $out_buf);"
		ensure
			instance_free: class
		end

end
