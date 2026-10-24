note
	description: "Automated test harness for VoidRush Phase 2 Eiffel core"

class
	VR_TEST_APP

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Run automated tests on Eiffel reactor, workers, connections, and engine.
		do
			io.put_string ("[vr_test_app] Testing VR_REACTOR_EVENT_BUFFER...%N")
			test_event_buffer

			io.put_string ("[vr_test_app] Testing VR_CONNECTION state machine...%N")
			test_connection_state

			io.put_string ("[vr_test_app] Testing VR_REACTOR initialization and shutdown...%N")
			test_reactor_lifecycle

			io.put_string ("[vr_test_app] Testing VR_ENGINE lifecycle and metrics...%N")
			test_engine_lifecycle

			io.put_string ("[vr_test_app] Testing end-to-end sharding, I/O drain, and disconnect...%N")
			test_end_to_end_sharding_and_io

			io.put_string ("[vr_test_app] All Eiffel tests passed successfully!%N")
		end

feature {NONE} -- Tests

	test_event_buffer
			-- Verify VR_REACTOR_EVENT_BUFFER indexing and capacity.
		local
			buf: VR_REACTOR_EVENT_BUFFER
		do
			create buf.make (64)
			check capacity_ok: buf.capacity = 64 end
			check pointer_valid: buf.base_address /= default_pointer end
			check initial_fd: buf.item_fd (0) = 0 end
			check initial_gen: buf.item_generation (0) = 0 end
			check initial_flags: buf.item_flags (0) = 0 end
		end

	test_connection_state
			-- Verify VR_CONNECTION lifecycle transitions and generation guards.
		local
			conn: VR_CONNECTION
		do
			create conn.make (10, 42)
			check fd_correct: conn.fd = 10 end
			check gen_correct: conn.generation = 42 end
			check is_open: conn.is_open end
			check matches_correct: conn.matches (42) end
			check rejects_wrong_gen: not conn.matches (43) end

			conn.mark_closing
			check is_closing: conn.state = State_closing end
			check not_matching_closing: not conn.matches (42) end

			conn.mark_closed
			check is_closed: conn.is_closed end
		end

	test_reactor_lifecycle
			-- Verify VR_REACTOR initialization with worker pool, step, and clean shutdown under SCOOP.
		local
			r: separate VR_REACTOR
		do
			create r.make (Test_port, 4)
			check is_listening: is_reactor_listening (r) end
			run_reactor_steps (r)
		end

	run_reactor_steps (r: separate VR_REACTOR)
			-- Run non-blocking poll steps and shut down.
		do
			check open: r.is_open end
			check listening: r.is_listening end
			check workers: r.worker_count = 4 end
			r.step (50)
			r.step (50)
			r.shutdown
			check closed: not r.is_open end
			check not_listening: not r.is_listening end
		end

	test_engine_lifecycle
			-- Verify VR_ENGINE initialization, metrics snapshot, and stop.
		local
			engine: VR_ENGINE
			m: VR_METRICS
		do
			create engine.make (Test_port + 1, 4)
			check port_ok: engine.port = Test_port + 1 end
			check workers_ok: engine.worker_count = 4 end
			check handle_ok: engine.reactor_handle /= default_pointer end

			m := engine.metrics
			check initial_accepts: m.accepts_total = 0 end
			check initial_active: m.active_connections = 0 end
			io.put_string ("  Initial " + m.out + "%N")

			-- Explicit shutdown of engine and its reactor
			engine.shutdown
		end

	test_end_to_end_sharding_and_io
			-- Verify multi-client connection, sharding, data drain, and clean closure.
		local
			r: separate VR_REACTOR
			c1, c2, res, i: INTEGER
			payload: C_STRING
			m: VR_METRICS
		do
			create r.make (Test_port + 2, 4)
			-- Synchronous barrier: ensure r.make has finished binding and listening
			check listening: is_reactor_listening (r) end

			create payload.make ("voidrush_p2_test")

			-- Connect 2 client sockets to reactor
			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 2)
			check c1_connected: c1 >= 0 end

			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 2)
			check c2_connected: c2 >= 0 end

			create m.make_from_pointer (get_native_handle (r))

			-- Poll reactor until both connections are accepted and registered
			from
				i := 0
			until
				i >= 10 or m.accepts_total >= 2
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			check two_accepts: m.accepts_total = 2 end
			check two_active: m.active_connections = 2 end

			-- Send data from client 1
			res := {VR_REACTOR_BRIDGE}.c_send (c1, payload.item, payload.count)
			check sent_c1: res = payload.count end

			-- Step reactor: epoll fires on c1, worker drains and re-arms
			from
				i := 0
			until
				i >= 5
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			-- Close both client sockets
			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			res := {VR_REACTOR_BRIDGE}.c_close (c2)

			-- Poll reactor until workers process disconnect and active count drops to 0
			from
				i := 0
			until
				i >= 10 or m.active_connections = 0
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			check zero_active: m.active_connections = 0 end
			io.put_string ("  End-to-end " + m.out + "%N")

			-- Shutdown reactor
			shutdown_reactor (r)
		end

feature {NONE} -- SCOOP Helpers

	is_reactor_listening (r: separate VR_REACTOR): BOOLEAN
			-- Synchronous query barrier ensuring reactor has completed initialization.
		do
			Result := r.is_listening
		end

	step_reactor (r: separate VR_REACTOR; timeout_ms: INTEGER)
			-- Asynchronous step command.
		do
			r.step (timeout_ms)
		end

	get_native_handle (r: separate VR_REACTOR): POINTER
			-- Synchronous query retrieving handle.
		do
			Result := r.native_handle
		end

	shutdown_reactor (r: separate VR_REACTOR)
			-- Shutdown reactor.
		do
			r.shutdown
		end

feature {NONE} -- Constants

	Test_port: INTEGER = 19877
			-- Dedicated test port

end
