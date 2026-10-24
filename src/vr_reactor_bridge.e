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

end
