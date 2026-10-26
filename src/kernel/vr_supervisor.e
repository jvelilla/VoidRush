note
	description: "OTP-inspired root supervisor coordinating child bootstrap, shutdown, and recovery"

class
	VR_SUPERVISOR

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_port, a_worker_count: INTEGER)
			-- Initialize supervisor and prepare child specifications.
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_workers: a_worker_count > 0 and a_worker_count <= Max_workers
		local
			spec: VR_CHILD_SPEC
			i: INTEGER
		do
			port := a_port
			worker_count := a_worker_count
			create intensity.make
			create child_specs.make (worker_count + 2)
			create workers.make_empty (worker_count)
			is_fatal_shutdown := False
			reactor_handle := default_pointer

			-- 1. Reactor spec
			create spec.make ("reactor", Child_reactor, -1, Restart_transient, 2000)
			child_specs.force (spec, "reactor")

			-- 2. Broker spec
			create spec.make ("broker", Child_broker, -1, Restart_permanent, 1000)
			child_specs.force (spec, "broker")

			-- 3. Worker specs
			from
				i := 0
			until
				i >= worker_count
			loop
				create spec.make ("worker_" + i.out, Child_worker, i, Restart_permanent, 1000)
				child_specs.force (spec, "worker_" + i.out)
				i := i + 1
			end
		ensure
			port_set: port = a_port
			worker_count_set: worker_count = a_worker_count
			not_fatal: not is_fatal_shutdown
			specs_initialized: child_specs.count = worker_count + 2
		end

feature -- Access

	port: INTEGER
			-- Listening port

	worker_count: INTEGER
			-- Number of worker shards

	reactor_handle: POINTER
			-- Direct pointer to native vr_reactor_t

	is_fatal_shutdown: BOOLEAN
			-- Has supervisor terminated due to unrecoverable or intensity-tripped failure?

	intensity: VR_RESTART_INTENSITY
			-- Sliding-window restart rate limiter

	child_spec (a_id: STRING_8): detachable VR_CHILD_SPEC
			-- Retrieve specification and status for child `a_id`.
		require
			valid_id: not a_id.is_empty
		do
			Result := child_specs.item (a_id)
		end

	reactor_state: INTEGER
			-- Lifecycle state of reactor component.
		do
			if attached child_specs.item ("reactor") as s then
				Result := s.state
			else
				Result := State_stopped
			end
		end

	broker_state: INTEGER
			-- Lifecycle state of broker component.
		do
			if attached child_specs.item ("broker") as s then
				Result := s.state
			else
				Result := State_stopped
			end
		end

	worker_state (a_index: INTEGER): INTEGER
			-- Lifecycle state of worker component `a_index`.
		do
			if attached child_specs.item ("worker_" + a_index.out) as s then
				Result := s.state
			else
				Result := State_stopped
			end
		end

	broker_restart_count: INTEGER
			-- Cumulative restart count of broker component.
		do
			if attached child_specs.item ("broker") as s then
				Result := s.last_restart_count
			end
		end

	worker_restart_count (a_index: INTEGER): INTEGER
			-- Cumulative restart count of worker component `a_index`.
		do
			if attached child_specs.item ("worker_" + a_index.out) as s then
				Result := s.last_restart_count
			end
		end

	is_running: BOOLEAN
			-- Is the supervisor actively managing running components?
		do
			Result := reactor_handle /= default_pointer
		end

feature -- Configuration

	set_restart_intensity (a_max_restarts: INTEGER; a_window_ms: NATURAL_64)
			-- Configure sliding-window restart intensity thresholds.
		require
			valid_max: a_max_restarts > 0
			valid_window: a_window_ms > 0
		do
			create intensity.make_custom (a_max_restarts, a_window_ms)
		ensure
			max_restarts_set: intensity.max_restarts = a_max_restarts
			window_set: intensity.window_ms = a_window_ms
		end

feature -- Lifecycle Coordination

	start_all
			-- Deterministic ordered startup sequence.
		require
			not_running: reactor_handle = default_pointer
		local
			w: separate VR_WORKER
			r: separate VR_REACTOR
			b: separate VR_BROKER
			i: INTEGER
		do
			-- 1. Allocate native C reactor
			reactor_handle := {VR_REACTOR_BRIDGE}.c_reactor_create
			check handle_created: reactor_handle /= default_pointer end

			-- 2. Construct Broker
			create b.make
			dispatch_broker_set_reactor_handle (b, reactor_handle)
			broker := b
			set_child_state ("broker", State_running)

			-- 3. Construct Reactor
			create r.make_with_components (port, reactor_handle, worker_count, b)
			reactor := r
			set_child_state ("reactor", State_running)

			-- 4. Construct Workers
			create workers.make_empty (worker_count)
			from
				i := 0
			until
				i >= worker_count
			loop
				create w.make (i, reactor_handle, b, worker_count)
				workers.extend (w)
				dispatch_register_worker (b, w)
				dispatch_register_reactor_worker (r, w)
				set_child_state ("worker_" + i.out, State_running)
				i := i + 1
			end
		ensure
			running: reactor_handle /= default_pointer
			reactor_running: attached child_specs.item ("reactor") as s1 and then s1.state = State_running
			broker_running: attached child_specs.item ("broker") as s2 and then s2.state = State_running
		end

	step (a_timeout_ms: INTEGER)
			-- Single event loop tick and health monitoring cycle.
		require
			running: is_running
		do
			if attached reactor as r then
				dispatch_reactor_step (r, a_timeout_ms)
			end
			check_health ({VR_REACTOR_BRIDGE}.c_reactor_now_ms)
			{VR_REACTOR_BRIDGE}.c_sleep_ms (a_timeout_ms)
		end

	run_loop
			-- Autonomous supervision and event loop until shutdown or fatal intensity trip.
		require
			running: is_running
		do
			from
			until
				not is_running or is_fatal_shutdown or {VR_REACTOR_BRIDGE}.c_reactor_is_shutting_down (reactor_handle) /= 0
			loop
				step (100)
			end
			if is_running then
				stop_all
			end
		end

	stop_all
			-- Deterministic reverse-order shutdown sequence.
		local
			i: INTEGER
			w: separate VR_WORKER
		do
			if reactor_handle /= default_pointer then
				-- 1. Signal native C shutdown & wake reactor loop
				{VR_REACTOR_BRIDGE}.c_reactor_request_shutdown (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)

				-- 2. Mark states stopping
				set_all_states (State_stopping)

				-- 3. Stop Reactor (unbinds listen socket)
				if attached reactor as r then
					dispatch_reactor_shutdown (r)
					check reactor_stopped: wait_reactor_shutdown (r) end
				end
				set_child_state ("reactor", State_stopped)

				-- 4. Teardown Workers (barrier wait)
				from
					i := 0
				until
					i >= workers.count
				loop
					w := workers.item (i)
					dispatch_worker_shutdown (w)
					check worker_stopped: wait_worker_shutdown (w) end
					set_child_state ("worker_" + i.out, State_stopped)
					i := i + 1
				end

				-- 5. Stop Broker
				if attached broker as b then
					dispatch_broker_shutdown (b)
					check broker_stopped: wait_broker_shutdown (b) end
					set_child_state ("broker", State_stopped)
				end

				-- 6. Destroy native reactor handle
				{VR_REACTOR_BRIDGE}.c_reactor_destroy (reactor_handle)
				reactor_handle := default_pointer
			end
		ensure
			stopped: reactor_handle = default_pointer
		end

feature -- Health Monitoring & Recovery

	check_health (a_now_ms: NATURAL_64)
			-- Poll lock-free C heartbeats and atomic fault flags; execute recovery actions.
		require
			valid_handle: reactor_handle /= default_pointer
			valid_time: a_now_ms > 0
		local
			fault_mask, bit: NATURAL_64
			r_beat, b_beat, w_beat: NATURAL_64
			i: INTEGER
		do
			-- 1. Consume atomic fault mask from C
			fault_mask := {VR_REACTOR_BRIDGE}.c_reactor_consume_fault_mask (reactor_handle)

			-- 2. Check Reactor health
			r_beat := {VR_REACTOR_BRIDGE}.c_reactor_get_reactor_beat (reactor_handle)
			if r_beat > 0 and then (a_now_ms > r_beat + Default_liveness_timeout_ms) then
				handle_reactor_failure
			else
				-- 3. Check Broker health & dispatch heartbeat ping
				if attached broker as b then
					dispatch_broker_supervisor_tick (b, a_now_ms)
				end
				b_beat := {VR_REACTOR_BRIDGE}.c_reactor_get_broker_beat (reactor_handle)
				bit := ({NATURAL_64} 1).bit_shift_left (63)
				if (fault_mask.bit_and (bit)) /= 0 or (b_beat > 0 and then a_now_ms > b_beat + Default_liveness_timeout_ms) then
					handle_broker_failure (a_now_ms)
				end

				-- 4. Check Worker shards health
				from
					i := 0
				until
					i >= worker_count or is_fatal_shutdown
				loop
					bit := ({NATURAL_64} 1).bit_shift_left (i)
					w_beat := {VR_REACTOR_BRIDGE}.c_reactor_get_worker_beat (reactor_handle, i)
					if (fault_mask.bit_and (bit)) /= 0 or (w_beat > 0 and then a_now_ms > w_beat + Default_liveness_timeout_ms) then
						handle_worker_failure (i, a_now_ms)
					end
					i := i + 1
				end
			end
		end

feature {NONE} -- Recovery Actions

	handle_broker_failure (a_now_ms: NATURAL_64)
			-- Restart broker in-place without dropping client connections.
		local
			new_b: separate VR_BROKER
			i: INTEGER
		do
			if intensity.allow_restart (a_now_ms) then
				intensity.record_restart (a_now_ms)
				{VR_REACTOR_BRIDGE}.c_metric_inc_restarts (reactor_handle)

				create new_b.make
				dispatch_broker_set_reactor_handle (new_b, reactor_handle)
				broker := new_b

				from
					i := 0
				until
					i >= workers.count
				loop
					dispatch_register_worker (new_b, workers.item (i))
					dispatch_set_broker (workers.item (i), new_b)
					i := i + 1
				end

				if attached child_specs.item ("broker") as s then
					s.record_restart
					s.set_state (State_running)
				end
			else
				io.put_string ("[supervisor] FATAL: Broker restart intensity exceeded (" + intensity.max_restarts.out + " restarts in " + (intensity.window_ms // 1000).out + "s). Halting system.%N")
				escalate_fatal_shutdown
			end
		end

	handle_worker_failure (a_index: INTEGER; a_now_ms: NATURAL_64)
			-- Purge shard FDs and hot-swap worker slot.
		require
			valid_index: a_index >= 0 and a_index < worker_count
		local
			new_w: separate VR_WORKER
			purged: INTEGER
		do
			if intensity.allow_restart (a_now_ms) then
				intensity.record_restart (a_now_ms)
				{VR_REACTOR_BRIDGE}.c_metric_inc_restarts (reactor_handle)

				-- 1. Purge all kernel sockets owned by crashed shard
				purged := {VR_REACTOR_BRIDGE}.c_reactor_purge_worker_fds (reactor_handle, a_index)

				-- 2. Create fresh worker instance
				if attached broker as b then
					create new_w.make (a_index, reactor_handle, b, worker_count)
					workers.put (new_w, a_index)

					-- 3. Hot-swap in broker & reactor
					dispatch_replace_broker_worker (b, a_index, new_w)
					if attached reactor as r then
						{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
						dispatch_replace_reactor_worker (r, a_index, new_w)
					end

					if attached child_specs.item ("worker_" + a_index.out) as s then
						s.record_restart
						s.set_state (State_running)
					end
				end
			else
				io.put_string ("[supervisor] FATAL: Worker #" + a_index.out + " restart intensity exceeded (" + intensity.max_restarts.out + " restarts in " + (intensity.window_ms // 1000).out + "s). Halting system.%N")
				escalate_fatal_shutdown
			end
		end

	handle_reactor_failure
			-- Reactor death cannot be recovered safely in-process; escalate.
		do
			escalate_fatal_shutdown
		end

	escalate_fatal_shutdown
			-- Intensity limit tripped or unrecoverable child failure; stop all and exit.
		do
			is_fatal_shutdown := True
			stop_all
		ensure
			fatal_marked: is_fatal_shutdown
		end

feature -- Testing & Diagnostics

	inject_worker_fault (a_index: INTEGER)
			-- For testing T7.3: arm isolated fault injection on worker `a_index`.
		require
			valid_index: a_index >= 0 and a_index < worker_count
		do
			dispatch_worker_fault_injection (workers.item (a_index))
		end

feature {NONE} -- Controlled SCOOP Helpers

	dispatch_worker_fault_injection (a_worker: separate VR_WORKER)
		do
			a_worker.inject_next_event_fault
		end

	dispatch_broker_set_reactor_handle (a_broker: separate VR_BROKER; a_handle: POINTER)
		do
			a_broker.set_reactor_handle (a_handle)
		end

	dispatch_register_reactor_worker (a_reactor: separate VR_REACTOR; a_worker: separate VR_WORKER)
		do
			a_reactor.register_worker (a_worker)
		end

	dispatch_register_worker (a_broker: separate VR_BROKER; a_worker: separate VR_WORKER)
		do
			a_broker.register_worker (a_worker)
		end

	dispatch_set_broker (a_worker: separate VR_WORKER; a_broker: separate VR_BROKER)
		do
			a_worker.set_broker (a_broker)
		end

	dispatch_replace_broker_worker (a_broker: separate VR_BROKER; a_index: INTEGER; a_worker: separate VR_WORKER)
		do
			a_broker.replace_worker (a_index, a_worker)
		end

	dispatch_broker_supervisor_tick (a_broker: separate VR_BROKER; a_now_ms: NATURAL_64)
		do
			a_broker.on_supervisor_tick (a_now_ms)
		end

	dispatch_reactor_step (a_reactor: separate VR_REACTOR; a_timeout_ms: INTEGER)
		do
			a_reactor.step (a_timeout_ms)
		end

	dispatch_replace_reactor_worker (a_reactor: separate VR_REACTOR; a_index: INTEGER; a_worker: separate VR_WORKER)
		do
			a_reactor.replace_worker (a_index, a_worker)
		end

	dispatch_worker_shutdown (a_worker: separate VR_WORKER)
		do
			a_worker.shutdown
		end

	wait_worker_shutdown (a_worker: separate VR_WORKER): BOOLEAN
		do
			Result := a_worker.is_shutdown_complete
		end

	dispatch_broker_shutdown (a_broker: separate VR_BROKER)
		do
			a_broker.shutdown
		end

	wait_broker_shutdown (a_broker: separate VR_BROKER): BOOLEAN
		do
			Result := a_broker.is_shutdown
		end

	dispatch_reactor_shutdown (a_reactor: separate VR_REACTOR)
		do
			a_reactor.shutdown
		end

	wait_reactor_shutdown (a_reactor: separate VR_REACTOR): BOOLEAN
		do
			Result := not a_reactor.is_open
		end

	launch_reactor_loop (a_reactor: separate VR_REACTOR)
		do
			a_reactor.run_loop
		end

	set_child_state (a_id: STRING_8; a_state: INTEGER)
		do
			if attached child_specs.item (a_id) as spec then
				spec.set_state (a_state)
			end
		end

	set_all_states (a_state: INTEGER)
		do
			across
				child_specs as spec
			loop
				spec.set_state (a_state)
			end
		end

feature {NONE} -- Child References & Specs

	child_specs: HASH_TABLE [VR_CHILD_SPEC, STRING_8]
	workers: SPECIAL [separate VR_WORKER]
	broker: detachable separate VR_BROKER
	reactor: detachable separate VR_REACTOR

invariant
	valid_port: port > 0 and port <= 65535
	valid_workers: worker_count > 0 and worker_count <= Max_workers

end
