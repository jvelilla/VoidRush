note
	description: "Separate SCOOP reactor coordinating listening, accept draining, and worker dispatch"

class
	VR_REACTOR

inherit
	VR_REACTOR_CONSTANTS

create
	make,
	make_with_components,
	make_with_host

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
			create broker.make

			from
				i := 0
			until
				i >= worker_count
			loop
				create w.make (i, handle, broker, worker_count)
				workers.extend (w)
				dispatch_register_worker (broker, w)
				i := i + 1
			end

			listen_fd := {VR_REACTOR_BRIDGE}.c_listen_tcp (default_pointer, a_port, Default_backlog)
			check listen_ok: listen_fd >= 0 end

			-- Generation 0 is reserved exclusively for the listening socket
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (handle, listen_fd, Reactor_in, 0)
			check listen_registered: res = 0 end

			last_tick_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
		ensure
			is_open: is_open
			is_listening: is_listening
			workers_initialized: workers.count = a_worker_count
		end

	make_with_components (a_port: INTEGER; a_handle: POINTER; a_worker_count: INTEGER; a_broker: separate VR_BROKER)
			-- Initialize reactor with externally managed components (Phase 7 supervision).
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_handle: a_handle /= default_pointer
			valid_worker_count: a_worker_count > 0
			valid_broker: a_broker /= Void
		do
			make_with_host (Void, a_port, a_handle, a_worker_count, a_broker)
		ensure
			is_open: is_open
			is_listening: is_listening
			broker_set: broker = a_broker
		end

	make_with_host (a_host: detachable separate READABLE_STRING_8; a_port: INTEGER; a_handle: POINTER; a_worker_count: INTEGER; a_broker: separate VR_BROKER)
			-- Initialize reactor with specific bind host and externally managed components.
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_handle: a_handle /= default_pointer
			valid_worker_count: a_worker_count > 0
			valid_broker: a_broker /= Void
		local
			res: INTEGER
			c_host: ANY
			host_ptr: POINTER
			bh: STRING_8
		do
			handle := a_handle
			worker_count := a_worker_count
			create workers.make_empty (a_worker_count)
			broker := a_broker
			is_externally_owned := True
			create events.make (Max_events)

			if attached a_host as h and then not h.is_empty then
				create bh.make_from_separate (h)
				bind_host := bh
				c_host := bh.to_c
				host_ptr := $c_host
			else
				host_ptr := default_pointer
			end

			listen_fd := {VR_REACTOR_BRIDGE}.c_listen_tcp (host_ptr, a_port, Default_backlog)
			check listen_ok: listen_fd >= 0 end

			-- Generation 0 is reserved exclusively for the listening socket
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (handle, listen_fd, Reactor_in, 0)
			check listen_registered: res = 0 end

			last_tick_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
		ensure
			is_open: is_open
			is_listening: is_listening
			broker_set: broker = a_broker
		end

	bind_host: detachable STRING_8
			-- Configured bind IP address or hostname

feature -- Registration

	register_worker (a_worker: separate VR_WORKER)
			-- Register worker shard with reactor for event dispatch.
		require
			valid_worker: a_worker /= Void
			capacity_available: registered_workers_count < worker_count
		do
			workers.extend (a_worker)
		ensure
			worker_added: registered_workers_count = old registered_workers_count + 1
		end

feature -- Access

	registered_workers_count: INTEGER
			-- Number of currently registered worker shards
		do
			Result := workers.count
		end

	native_handle: POINTER
			-- Direct pointer to native vr_reactor_t (queried by VR_ENGINE for atomic shutdown)
		do
			Result := handle
		end

	worker_count: INTEGER
			-- Number of worker partitions

feature -- Configuration

	set_protocol (a_proto: INTEGER)
			-- Configure protocol mode across all worker processors.
		require
			valid_proto: a_proto >= Proto_echo and a_proto <= Proto_http_reading_body
		local
			i: INTEGER
		do
			from
				i := 0
			until
				i >= workers.count
			loop
				dispatch_set_protocol (workers.item (i), a_proto)
				i := i + 1
			end
		end

	replace_worker (a_index: INTEGER; a_worker: separate VR_WORKER)
			-- Hot-swap worker slot `a_index` in reactor dispatch array.
		require
			valid_index: a_index >= 0 and a_index < worker_count
			valid_worker: a_worker /= Void
		do
			workers.put (a_worker, a_index)
		ensure
			worker_replaced: workers.item (a_index) = a_worker
		end

	set_broker (a_broker: separate VR_BROKER)
			-- Update shared broker reference upon supervisor broker restart.
		require
			valid_broker: a_broker /= Void
		do
			broker := a_broker
		ensure
			broker_set: broker = a_broker
		end

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
			-- Single poll tick, event dispatch cycle, and periodic timer tick.
		require
			is_open: is_open
		local
			n, i, fd, target_index: INTEGER
			gen, flags: NATURAL_32
			target_worker: separate VR_WORKER
			now_ms: NATURAL_64
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
						target_index := {VR_REACTOR_BRIDGE}.c_reactor_route_fd (handle, fd, worker_count)
						if target_index >= 0 and target_index < workers.count then
							target_worker := workers.item (target_index)
							dispatch_event (target_worker, fd, gen, flags)
						else
							{VR_REACTOR_BRIDGE}.c_metric_inc_stale (handle)
						end
					end
					i := i + 1
				end
			end

			-- Periodic worker idle sweep tick and reactor liveness heartbeat
			now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
			{VR_REACTOR_BRIDGE}.c_reactor_beat_reactor (handle, now_ms)

			if now_ms >= last_tick_ms + Timer_tick_interval_ms.to_natural_64 then
				last_tick_ms := now_ms
				from
					i := 0
				until
					i >= workers.count
				loop
					dispatch_timer_tick (workers.item (i), now_ms)
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

			if not is_externally_owned then
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

				-- 4. Workers drained; safely terminate pub/sub broker
				dispatch_broker_shutdown (broker)

				-- 5. Destroy native reactor memory once all workers are confirmed idle
				if handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_reactor_destroy (handle)
				end
			end
			handle := default_pointer
		ensure
			cleaned_up: not is_open and not is_listening
		end

feature {NONE} -- SCOOP Controlled Dispatch Helpers

	dispatch_set_protocol (a_worker: separate VR_WORKER; a_proto: INTEGER)
			-- Asynchronous command configuring worker protocol mode.
		do
			a_worker.set_protocol (a_proto)
		end

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

	dispatch_timer_tick (a_worker: separate VR_WORKER; a_now_ms: NATURAL_64)
			-- Asynchronous timer tick command to controlled worker argument.
		do
			a_worker.on_timer_tick (a_now_ms)
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

	dispatch_broker_shutdown (a_broker: separate VR_BROKER)
			-- Asynchronous shutdown command to controlled broker argument.
		do
			a_broker.shutdown
		end

	dispatch_register_worker (a_broker: separate VR_BROKER; a_worker: separate VR_WORKER)
			-- Asynchronous registration command to controlled broker argument.
		do
			a_broker.register_worker (a_worker)
		end

feature {NONE} -- Implementation

	accept_pending_connections
			-- Accept up to Accept_budget_per_turn queued clients to prevent starvation.
		local
			client, res, count, target_index: INTEGER
			done: BOOLEAN
			target_worker: separate VR_WORKER
		do
			from
				done := False
				count := 0
			until
				done or count >= Accept_budget_per_turn
			loop
				client := {VR_REACTOR_BRIDGE}.c_accept_nonblocking (listen_fd)
				if client >= 0 then
					count := count + 1
					next_generation := next_generation + 1
					if next_generation = 0 then
						next_generation := 1 -- 0 is reserved for listen_fd
					end
					{VR_REACTOR_BRIDGE}.c_metric_inc_accept (handle)
					target_index := {VR_REACTOR_BRIDGE}.c_reactor_assign_worker (handle, client, worker_count)
					target_worker := workers.item (target_index)
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
	broker: separate VR_BROKER
	next_generation: NATURAL_32
	last_tick_ms: NATURAL_64
	is_externally_owned: BOOLEAN

invariant
	valid_listen_socket: listen_fd >= -1
	valid_worker_count: worker_count > 0
	workers_present: workers.count <= worker_count

end
