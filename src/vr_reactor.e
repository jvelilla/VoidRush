note
	description: "Separate SCOOP reactor coordinating listening, accept draining, and worker dispatch"

class
	VR_REACTOR

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_port, a_worker_count: INTEGER)
			-- Initialize reactor on `a_port` with `a_worker_count` separate workers.
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_workers: a_worker_count > 0
		local
			res, i: INTEGER
			w: separate VR_WORKER
		do
			worker_count := a_worker_count
			handle := {VR_REACTOR_BRIDGE}.c_reactor_create
			check reactor_created: handle /= default_pointer end

			create events.make (Max_events)
			create workers.make_empty (worker_count)

			from
				i := 0
			until
				i >= worker_count
			loop
				create w.make (i, handle)
				workers.extend (w)
				i := i + 1
			end

			listen_fd := {VR_REACTOR_BRIDGE}.c_listen_tcp (default_pointer, a_port, Default_backlog)
			check listen_ok: listen_fd >= 0 end

			-- Generation 0 is reserved exclusively for the listening socket
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (handle, listen_fd, Reactor_in, 0)
			check listen_registered: res = 0 end
		ensure
			is_open: is_open
			is_listening: is_listening
			workers_initialized: workers.count = a_worker_count
		end

feature -- Access

	native_handle: POINTER
			-- Direct pointer to native vr_reactor_t (queried by VR_ENGINE for atomic shutdown)
		do
			Result := handle
		end

	worker_count: INTEGER
			-- Number of worker partitions

feature -- Status Report

	is_open: BOOLEAN
			-- Is native reactor open and active?
		do
			Result := handle /= default_pointer
		end

	is_listening: BOOLEAN
			-- Is listening socket active?
		do
			Result := listen_fd >= 0
		end

feature -- Execution

	run_loop
			-- Autonomous polling loop until native shutdown signal is received.
		require
			is_open: is_open
		do
			from
			until
				{VR_REACTOR_BRIDGE}.c_reactor_is_shutting_down (handle) /= 0
			loop
				step (100)
			end
			perform_graceful_teardown
		ensure
			is_closed: not is_open and not is_listening
		end

	step (timeout_ms: INTEGER)
			-- Single poll tick and event dispatch cycle.
		require
			is_open: is_open
		local
			n, i, fd: INTEGER
			gen, flags: NATURAL_32
			target_worker: separate VR_WORKER
		do
			n := {VR_REACTOR_BRIDGE}.c_reactor_wait (handle, events.base_address, Max_events, timeout_ms)
			if n > 0 then
				{VR_REACTOR_BRIDGE}.c_metric_inc_events (handle, n.to_natural_64)
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
						target_worker := workers.item (fd \\ worker_count)
						dispatch_event (target_worker, fd, gen, flags)
					end
					i := i + 1
				end
			end
		end

	shutdown
			-- Explicit teardown (invoked when not using run_loop).
		do
			if is_open or is_listening then
				perform_graceful_teardown
			end
		ensure
			is_closed: not is_open and not is_listening
		end

feature {NONE} -- Teardown & Barriers

	perform_graceful_teardown
			-- Synchronized barrier shutdown of listen socket, all workers, and native reactor.
		local
			i, res: INTEGER
			w: separate VR_WORKER
			completed: BOOLEAN
		do
			-- 1. Deregister and close listening socket
			if listen_fd >= 0 then
				res := {VR_REACTOR_BRIDGE}.c_reactor_del (handle, listen_fd)
				res := {VR_REACTOR_BRIDGE}.c_close (listen_fd)
				listen_fd := -1
			end

			-- 2. Dispatch asynchronous shutdown to all workers
			from
				i := 0
			until
				i >= workers.count
			loop
				w := workers.item (i)
				dispatch_shutdown (w)
				i := i + 1
			end

			-- 3. Synchronous wait-by-necessity barrier for each worker
			from
				i := 0
			until
				i >= workers.count
			loop
				w := workers.item (i)
				completed := wait_worker_shutdown (w)
				check worker_done: completed end
				i := i + 1
			end

			-- 4. Destroy native reactor memory once all workers are confirmed idle
			if handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_destroy (handle)
				handle := default_pointer
			end
		ensure
			cleaned_up: not is_open and not is_listening
		end

feature {NONE} -- SCOOP Controlled Dispatch Helpers

	dispatch_register (a_worker: separate VR_WORKER; a_fd: INTEGER; a_gen: NATURAL_32)
			-- Asynchronous registration command to controlled worker argument.
		do
			a_worker.register_connection (a_fd, a_gen)
		end

	dispatch_event (a_worker: separate VR_WORKER; a_fd: INTEGER; a_gen, a_flags: NATURAL_32)
			-- Asynchronous event command to controlled worker argument.
		do
			a_worker.on_socket_event (a_fd, a_gen, a_flags)
		end

	dispatch_shutdown (a_worker: separate VR_WORKER)
			-- Asynchronous shutdown command to controlled worker argument.
		do
			a_worker.shutdown
		end

	wait_worker_shutdown (a_worker: separate VR_WORKER): BOOLEAN
			-- Synchronous query barrier waiting for worker shutdown completion.
		do
			Result := a_worker.is_shutdown_complete
		end

feature {NONE} -- Implementation

	accept_pending_connections
			-- Accept all queued clients until WOULD_BLOCK and dispatch to assigned workers.
		local
			client, res: INTEGER
			done: BOOLEAN
			target_worker: separate VR_WORKER
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
					{VR_REACTOR_BRIDGE}.c_metric_inc_accept (handle)
					target_worker := workers.item (client \\ worker_count)
					dispatch_register (target_worker, client, next_generation)
				else
					done := True
				end
			end
			-- Re-arm listen_fd for next connection burst
			res := {VR_REACTOR_BRIDGE}.c_reactor_mod (handle, listen_fd, Reactor_in, 0)
			check listen_rearmed: res = 0 end
		end


	handle: POINTER
	listen_fd: INTEGER
	events: VR_REACTOR_EVENT_BUFFER
	workers: SPECIAL [separate VR_WORKER]
	next_generation: NATURAL_32

invariant
	valid_listen_socket: listen_fd >= -1
	valid_worker_count: worker_count > 0
	workers_present: workers.count = worker_count

end
