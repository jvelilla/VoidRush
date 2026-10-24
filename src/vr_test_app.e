note
	description: "Automated test harness for VoidRush Phase 3 TCP Echo and Phase 4 HTTP/1.1 Engine"

class
	VR_TEST_APP

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Run automated tests on Eiffel reactor, workers, connections, and HTTP engine.
		do
			io.put_string ("[vr_test_app] Testing VR_REACTOR_EVENT_BUFFER...%N")
			test_event_buffer

			io.put_string ("[vr_test_app] Testing VR_CONNECTION state machine, buffer compaction, and interest...%N")
			test_connection_state

			io.put_string ("[vr_test_app] Testing VR_HTTP_SLICE character matches and conversions...%N")
			test_http_slice

			io.put_string ("[vr_test_app] Testing VR_HTTP_PARSER incremental header scanner...%N")
			test_http_parser

			io.put_string ("[vr_test_app] Testing VR_PROXY_V2 binary preface decoding...%N")
			test_proxy_v2

			io.put_string ("[vr_test_app] Testing VR_REACTOR initialization and shutdown...%N")
			test_reactor_lifecycle

			io.put_string ("[vr_test_app] Testing VR_ENGINE lifecycle and metrics...%N")
			test_engine_lifecycle

			io.put_string ("[vr_test_app] Testing end-to-end non-blocking TCP echo (Proto_echo mode)...%N")
			test_end_to_end_echo

			io.put_string ("[vr_test_app] Testing peer half-close (shutdown WR / FIN) echo and clean drain...%N")
			test_half_closed_echo

			io.put_string ("[vr_test_app] Testing end-to-end HTTP/1.1 /healthz fast path...%N")
			test_end_to_end_http_healthz

			io.put_string ("[vr_test_app] Testing end-to-end HTTP/1.1 request pipelining...%N")
			test_end_to_end_http_pipelining

			io.put_string ("[vr_test_app] Testing end-to-end HTTP/1.1 oversize header 431 rejection...%N")
			test_end_to_end_http_oversize_header

			io.put_string ("[vr_test_app] All Eiffel Phase 3 and Phase 4 tests passed successfully!%N")
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
			create conn.make_with_proto (10, 42, now_ms, Proto_echo)
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

	test_http_slice
			-- Verify VR_HTTP_SLICE string comparison, prefix, and integer parsing.
		local
			buf: MANAGED_POINTER
			s1, s2, s3: VR_HTTP_SLICE
			str: STRING_8
		do
			str := "GET /healthz?v=1 HTTP/1.1%R%NContent-Length: 12345%R%N"
			create buf.make (str.count)
			across 1 |..| str.count as ic loop
				buf.put_character (str.item (ic), ic - 1)
			end

			create s1.make (0, 3) -- "GET"
			check s1_same: s1.same_as_string (buf, "GET") end
			check s1_not_same: not s1.same_as_string (buf, "POST") end

			create s2.make (4, 8) -- "/healthz"
			check s2_same: s2.same_as_string (buf, "/healthz") end
			check s2_prefix: s2.starts_with (buf, "/health") end

			create s3.make (43, 5) -- "12345"
			check s3_int: s3.to_integer (buf) = 12345 end
		end

	test_http_parser
			-- Verify incremental parsing of request line and headers.
		local
			parser: VR_HTTP_PARSER
			buf: MANAGED_POINTER
			req: STRING_8
			ok: BOOLEAN
		do
			create parser.make
			req := "GET /healthz HTTP/1.1%R%NHost: localhost%R%NConnection: keep-alive%R%N%R%N"
			create buf.make (req.count)
			across 1 |..| req.count as ic loop
				buf.put_character (req.item (ic), ic - 1)
			end

			ok := parser.try_parse_request_header (buf, 0, req.count)
			check parse_ok: ok end
			check method_get: parser.method = Method_get end
			check path_healthz: parser.path_slice.same_as_string (buf, "/healthz") end
			check keep_alive: parser.is_keep_alive end
			check host_set: parser.host_slice.same_as_string (buf, "localhost") end
			check consumed_correct: parser.header_bytes_consumed = req.count end

			-- Incomplete header check
			parser.reset
			ok := parser.try_parse_request_header (buf, 0, 20)
			check incomplete_rejected: not ok end
		end

	test_proxy_v2
			-- Verify decoding of HAProxy PROXY Protocol v2 IPv4 header.
		local
			p: VR_PROXY_V2
			buf: MANAGED_POINTER
			ok: BOOLEAN
		do
			create p.make
			create buf.make (28)
			-- 12-byte magic: \x0D \x0A \x0D \x0A \x00 \x0D \x0A \x51 \x55 \x49 \x54 \x0A
			buf.put_natural_8 (0x0D, 0); buf.put_natural_8 (0x0A, 1)
			buf.put_natural_8 (0x0D, 2); buf.put_natural_8 (0x0A, 3)
			buf.put_natural_8 (0x00, 4); buf.put_natural_8 (0x0D, 5)
			buf.put_natural_8 (0x0A, 6); buf.put_natural_8 (0x51, 7)
			buf.put_natural_8 (0x55, 8); buf.put_natural_8 (0x49, 9)
			buf.put_natural_8 (0x54, 10); buf.put_natural_8 (0x0A, 11)
			-- ver/cmd: 0x21 (PROXY)
			buf.put_natural_8 (0x21, 12)
			-- fam/proto: 0x11 (AF_INET, STREAM)
			buf.put_natural_8 (0x11, 13)
			-- len: 12 (0x00, 0x0C)
			buf.put_natural_8 (0x00, 14); buf.put_natural_8 (0x0C, 15)
			-- src IP: 192.168.1.100
			buf.put_natural_8 (192, 16); buf.put_natural_8 (168, 17)
			buf.put_natural_8 (1, 18); buf.put_natural_8 (100, 19)
			-- dst IP: 127.0.0.1
			buf.put_natural_8 (127, 20); buf.put_natural_8 (0, 21)
			buf.put_natural_8 (0, 22); buf.put_natural_8 (1, 23)
			-- src port: 8443 (0x20FB)
			buf.put_natural_8 (0x20, 24); buf.put_natural_8 (0xFB, 25)
			-- dst port: 8080 (0x1F90)
			buf.put_natural_8 (0x1F, 26); buf.put_natural_8 (0x90, 27)

			ok := p.try_parse (buf, 0, 28)
			check proxy_ok: ok end
			check proxy_ip: p.client_ip.is_equal ("192.168.1.100") end
			check proxy_port: p.client_port = 8443 end
			check proxy_consumed: p.header_bytes_consumed = 28 end
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
			check initial_events: m.events_total = 0 end
			check initial_stale: m.stale_events = 0 end
			check initial_bytes_in: m.bytes_in_total = 0 end
			check initial_bytes_out: m.bytes_out_total = 0 end
			check initial_idle_drops: m.idle_timeouts_total = 0 end

			io.put_string ("  Initial " + m.out + "%N")
			engine.stop
		end

	test_end_to_end_echo
			-- Verify bidirectional echo with non-blocking read/write loops under fair budgets.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			payload: C_STRING
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
		do
			create r.make (Test_port + 2, 4)
			check listening: is_reactor_listening (r) end
			set_reactor_protocol (r, Proto_echo)

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
				i >= 30 or total_read >= payload.count
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
			set_reactor_protocol (r, Proto_echo)

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

	test_end_to_end_http_healthz
			-- Verify full HTTP/1.1 request/response round-trip to /healthz fast-path.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			req: C_STRING
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
			resp_str: STRING_8
		do
			create r.make (Test_port + 4, 4)
			check listening: is_reactor_listening (r) end

			create req.make ("GET /healthz HTTP/1.1%R%NHost: localhost%R%N%R%N")
			create recv_buf.make (256)

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 4)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			create m.make_from_pointer (get_native_handle (r))

			-- Poll until accepted
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

			-- Send HTTP GET /healthz
			res := {VR_REACTOR_BRIDGE}.c_send (c1, req.item, req.count)
			check sent_ok: res = req.count end

			-- Step reactor and read response
			from
				i := 0
				total_read := 0
			until
				i >= 30 or total_read > 0
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (c1, recv_buf.item, 256)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end

			check got_response: total_read > 0 end
			recv_buf.put_integer_8 (0, total_read)
			resp_str := (create {C_STRING}.make_by_pointer (recv_buf.item)).string
			check status_200: resp_str.has_substring ("HTTP/1.1 200 OK") end
			check body_ok: resp_str.has_substring ("%R%N%R%NOK") end

			m.refresh (get_native_handle (r))
			check bytes_in_tracked: m.bytes_in_total >= req.count.to_natural_64 end
			check bytes_out_tracked: m.bytes_out_total >= total_read.to_natural_64 end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_http_pipelining
			-- Verify two pipelined HTTP requests sent in one TCP burst receive both responses in order.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			req: C_STRING
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
			resp_str: STRING_8
		do
			create r.make (Test_port + 5, 4)
			check listening: is_reactor_listening (r) end

			create req.make ("GET /healthz HTTP/1.1%R%NHost: localhost%R%N%R%NGET /echo HTTP/1.1%R%NHost: localhost%R%N%R%N")
			create recv_buf.make (1024)

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 5)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			create m.make_from_pointer (get_native_handle (r))

			from
				i := 0
			until
				i >= 20 or (m.accepts_total >= 1 and m.active_connections >= 1)
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			-- Send pipelined requests
			res := {VR_REACTOR_BRIDGE}.c_send (c1, req.item, req.count)
			check sent_ok: res = req.count end

			-- Step reactor and drain both responses
			from
				i := 0
				total_read := 0
			until
				i >= 40 or total_read >= 150
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (c1, recv_buf.item + total_read, 1024 - total_read)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end

			check pipelined_received: total_read >= 100 end
			recv_buf.put_integer_8 (0, total_read)
			resp_str := (create {C_STRING}.make_by_pointer (recv_buf.item)).string
			-- Verify response 1 (healthz) and response 2 (echo) are both present
			check resp1_ok: resp_str.has_substring ("HTTP/1.1 200 OK") end
			check resp2_ok: resp_str.has_substring ("VoidRush HTTP/1.1 Engine OK") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_http_oversize_header
			-- Verify headers exceeding 16 KiB receive 431 Request Header Fields Too Large.
		local
			r: separate VR_REACTOR
			c1, res, i, n, total_read: INTEGER
			chunk: MANAGED_POINTER
			recv_buf: MANAGED_POINTER
			m: VR_METRICS
			resp_str: STRING_8
		do
			create r.make (Test_port + 6, 4)
			check listening: is_reactor_listening (r) end

			create chunk.make (4096)
			across 0 |..| 4095 as ic loop
				chunk.put_character ('X', ic)
			end
			create recv_buf.make (256)

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 6)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			create m.make_from_pointer (get_native_handle (r))

			from
				i := 0
			until
				i >= 20 or (m.accepts_total >= 1 and m.active_connections >= 1)
			loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			-- Send 5 x 4 KiB chunks = 20 KiB (> 16 KiB limit) without delimiter
			from
				i := 0
			until
				i >= 5
			loop
				res := {VR_REACTOR_BRIDGE}.c_send (c1, chunk.item, chunk.count)
				step_reactor (r, 50)
				i := i + 1
			end

			-- Step reactor and read response
			from
				i := 0
				total_read := 0
			until
				i >= 30 or total_read > 0
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (c1, recv_buf.item, 256)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end

			check got_431: total_read > 0 end
			recv_buf.put_integer_8 (0, total_read)
			resp_str := (create {C_STRING}.make_by_pointer (recv_buf.item)).string
			check is_431: resp_str.has_substring ("431 Request Header Fields Too Large") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

feature {NONE} -- SCOOP Helpers

	is_reactor_listening (r: separate VR_REACTOR): BOOLEAN
			-- Synchronous query barrier ensuring reactor has completed initialization.
		do
			Result := r.is_listening
		end

	set_reactor_protocol (r: separate VR_REACTOR; a_proto: INTEGER)
			-- Asynchronous protocol configuration command.
		do
			r.set_protocol (a_proto)
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
			-- Dedicated test port base

end
