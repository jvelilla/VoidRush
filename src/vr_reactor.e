note
	description: "Separate SCOOP reactor coordinating non-blocking event polling and connection accepts"

class
	VR_REACTOR

inherit
	DISPOSABLE

create
	make

feature {NONE} -- Initialization

	make (port: INTEGER)
			-- Initialize reactor listening on `port`.
		require
			valid_port: port > 0 and port <= 65535
		local
			res: INTEGER
		do
			handle := {VR_REACTOR_BRIDGE}.c_reactor_create
			check reactor_created: handle /= default_pointer end

			create events.make ({VR_REACTOR_CONSTANTS}.Max_events)

			listen_fd := {VR_REACTOR_BRIDGE}.c_listen_tcp (default_pointer, port, 1024)
			check listen_ok: listen_fd >= 0 end

			-- Generation 0 is reserved for the listening socket
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (handle, listen_fd, {VR_REACTOR_CONSTANTS}.Reactor_in, 0)
			check listen_registered: res = 0 end
		ensure
			is_initialized: is_open and is_listening
		end

feature -- Status report

	is_open: BOOLEAN
			-- Is reactor initialized and open?
		do
			Result := handle /= default_pointer
		end

	is_listening: BOOLEAN
			-- Is reactor actively listening for connections?
		do
			Result := listen_fd >= 0
		end

feature -- Execution

	step (timeout_ms: INTEGER)
			-- Poll for I/O readiness and process events.
		require
			is_open: is_open
		local
			n, i: INTEGER
			fd: INTEGER
			gen, flags: NATURAL_32
		do
			n := {VR_REACTOR_BRIDGE}.c_reactor_wait (handle, events.base_address, {VR_REACTOR_CONSTANTS}.Max_events, timeout_ms)
			if n > 0 then
				from
					i := 0
				until
					i >= n
				loop
					fd := events.item_fd (i)
					if fd = listen_fd then
						accept_pending_connections
					else
						gen := events.item_generation (i)
						flags := events.item_flags (i)
						-- Phase 2: Dispatch to separate WORKER processor
						-- worker_pool.item (fd \\ worker_count).on_event (fd, gen, flags)
						io.put_string ("Event on fd=" + fd.out + " gen=" + gen.out + " flags=" + flags.out + "%N")
					end
					i := i + 1
				end
			end
		end

	shutdown
			-- Close listening socket and release native reactor resources.
		local
			res: INTEGER
		do
			if listen_fd >= 0 then
				res := {VR_REACTOR_BRIDGE}.c_close (listen_fd)
				listen_fd := -1
			end
			if handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_destroy (handle)
				handle := default_pointer
			end
		ensure
			is_closed: not is_open and not is_listening
		end

feature {NONE} -- Removal

	dispose
			-- Safeguard resource reclamation if shutdown was not invoked explicitly.
		do
			if handle /= default_pointer or listen_fd >= 0 then
				shutdown
			end
		end

feature {NONE} -- Implementation

	accept_pending_connections
			-- Drain all queued connections until WOULD_BLOCK to avoid EPOLLET starvation.
		local
			client, res: INTEGER
			done: BOOLEAN
		do
			from
				done := False
			until
				done
			loop
				client := {VR_REACTOR_BRIDGE}.c_accept_nonblocking (listen_fd)
				if client >= 0 then
					next_generation := next_generation + 1
					if next_generation = 0 then
						next_generation := 1 -- 0 is reserved for listen_fd
					end
					res := {VR_REACTOR_BRIDGE}.c_reactor_add (handle, client, {VR_REACTOR_CONSTANTS}.Reactor_in, next_generation)
					io.put_string ("Accepted fd=" + client.out + " gen=" + next_generation.out + "%N")
				else
					-- Returned WOULD_BLOCK (-1) or fatal error (-2)
					done := True
				end
			end
			-- Re-arm listen_fd for subsequent connection bursts
			res := {VR_REACTOR_BRIDGE}.c_reactor_mod (handle, listen_fd, {VR_REACTOR_CONSTANTS}.Reactor_in, 0)
			check listen_rearmed: res = 0 end
		end

	handle: POINTER
			-- Pointer to native vr_reactor_t structure

	listen_fd: INTEGER
			-- Listening socket file descriptor

	events: VR_REACTOR_EVENT_BUFFER
			-- Pre-allocated event buffer

	next_generation: NATURAL_32
			-- Incrementing generation token counter

invariant
	valid_listen_socket: listen_fd >= -1

end
