note
	description: "Automated test harness for VoidRush Phase 3 TCP Echo and State Machine"

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

			io.put_string ("[vr_test_app] Testing VR_CONNECTION state machine, buffer compaction, and interest...%N")
			test_connection_state

			io.put_string ("[vr_test_app] Testing VR_REACTOR initialization and shutdown...%N")
			test_reactor_lifecycle

			io.put_string ("[vr_test_app] Testing VR_ENGINE lifecycle and metrics...%N")
			test_engine_lifecycle

			io.put_string ("[vr_test_app] Testing end-to-end non-blocking TCP echo...%N")
			test_end_to_end_echo

			io.put_string ("[vr_test_app] Testing peer half-close (shutdown WR / FIN) echo and clean drain...%N")
			test_half_closed_echo

			io.put_string ("[vr_test_app] All Eiffel Phase 3 tests passed successfully!%N")
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
			-- Verify VR_CONNECTION lifecycle transitions, buffer compaction, and interest flags.
		local
			conn: VR_CONNECTION
			scratch: MANAGED_POINTER
			now_ms: NATURAL_64
		do
			now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
			create conn.make_with_time (10, 42, now_ms)
			check fd_correct: conn.fd = 10 end
			check gen_correct: conn.generation = 42 end
			check is_open: conn.is_open end
			check matches_correct: conn.matches (42) end
			check rejects_wrong_gen: not conn.matches (43) end
			check initial_interest: conn.interest_flags = Reactor_in end

			-- Test appending to echo buffer
			create scratch.make (64)
			scratch.put_integer_8 (65, 0) -- 'A'
			scratch.put_integer_8 (66, 1) -- 'B'
			scratch.put_integer_8 (67, 2) -- 'C'
			conn.append_echo (scratch.item, 3)
			check bytes_appended: conn.out_buf_bytes = 3 end
			check interest_with_out: conn.interest_flags = (Reactor_in | Reactor_out) end

			-- Partial read
			conn.advance_read (1)
			check bytes_remaining: conn.out_buf_bytes = 2 end

			-- Finish reading -> offset reset to 0
			conn.advance_read (2)
			check buffer_empty: conn.out_buf_bytes = 0 end
			check interest_empty: conn.interest_flags = Reactor_in end

			-- Test compaction: append 10 bytes, read 5, append 10 more
			conn.append_echo (scratch.item, 10)
			conn.advance_read (5)
			conn.append_echo (scratch.item, 10)
			check compacted_count: conn.out_buf_bytes = 15 end

			-- Test idle detection
			check not_idle: not conn.is_idle (now_ms, 1000) end
			check is_idle: conn.is_idle (now_ms + 70000, 60000) end

			-- Test closing transitions
			conn.mark_closing
			check is_closing: conn.is_closing end
			check matches_closing: conn.matches (42) end
			-- When closing with bytes pending, only Reactor_out is armed
			check interest_closing_out: conn.interest_flags = Reactor_out end

			conn.advance_read (15)
			check buffer_drained: conn.out_buf_bytes = 0 end
			-- Drained while closing: interest is 0 (socket is closed, never re-armed)
			check interest_closing_drained: conn.interest_flags = 0 end

			conn.mark_closed
			check is_closed: conn.is_closed end
			check not_matches_closed: not conn.matches (42) end
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

			engine.shutdown
		end

	test_end_to_end_echo
			-- Verify multi-client connection, sharding, and full byte-preserving echo.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			payload: C_STRING
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
		do
			create r.make (Test_port + 2, 4)
			check listening: is_reactor_listening (r) end

			create payload.make ("VoidRush_Phase3_Echo_Verification_1234567890")
			create recv_buf.make (128)

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 2)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			create m.make_from_pointer (get_native_handle (r))

			-- Poll reactor until client is registered and armed by worker
			from
				i := 0
			until
				i >= 20 or (m.accepts_total >= 1 and m.active_connections >= 1)
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check client_accepted: m.accepts_total = 1 and m.active_connections = 1 end

			-- Send payload from client to server
			res := {VR_REACTOR_BRIDGE}.c_send (c1, payload.item, payload.count)
			check sent_ok: res = payload.count end

			-- Step reactor: epoll fires on worker, worker reads and echoes
			from
				i := 0
				total_read := 0
			until
				i >= 50 or total_read >= payload.count
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (c1, recv_buf.item + total_read, payload.count - total_read)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end

			check echo_received_all: total_read = payload.count end
			recv_buf.put_integer_8 (0, total_read)
			check content_matches: (create {C_STRING}.make_by_pointer (recv_buf.item)).string.is_equal (payload.string) end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)

			-- Poll until closed
			from
				i := 0
			until
				i >= 30 or m.active_connections = 0
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check zero_active: m.active_connections = 0 end

			shutdown_reactor (r)
		end

	test_half_closed_echo
			-- Verify peer half-close: client sends payload and immediately issues shutdown(WR).
			-- Server reads all payload bytes, echoes them completely, and closes cleanly when out_buf drains.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			payload: C_STRING
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
		do
			create r.make (Test_port + 3, 4)
			check listening: is_reactor_listening (r) end

			create payload.make ("Half_Closed_Payload_Before_FIN")
			create recv_buf.make (128)

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 3)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			create m.make_from_pointer (get_native_handle (r))

			-- Poll until accepted and armed by worker
			from
				i := 0
			until
				i >= 20 or (m.accepts_total >= 1 and m.active_connections >= 1)
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check client_accepted: m.accepts_total = 1 and m.active_connections = 1 end

			-- Send payload and immediately half-close write direction (FIN)
			res := {VR_REACTOR_BRIDGE}.c_send (c1, payload.item, payload.count)
			check sent_ok: res = payload.count end
			res := {VR_REACTOR_BRIDGE}.c_shutdown_wr (c1)
			check shutdown_wr_ok: res = 0 end

			-- Step reactor: kernel delivers IN | HUP. Server reads payload, transitions to Closing,
			-- writes echo, and closes once drained.
			from
				i := 0
				total_read := 0
			until
				i >= 50 or total_read >= payload.count
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (c1, recv_buf.item + total_read, payload.count - total_read)
				if n > 0 then
					total_read := total_read + n
				end
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			check echo_received_on_half_close: total_read = payload.count end
			recv_buf.put_integer_8 (0, total_read)
			check content_matches: (create {C_STRING}.make_by_pointer (recv_buf.item)).string.is_equal (payload.string) end

			-- Server should now have closed the connection
			from
				i := 0
			until
				i >= 30 or m.active_connections = 0
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check closed_on_server: m.active_connections = 0 end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
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
