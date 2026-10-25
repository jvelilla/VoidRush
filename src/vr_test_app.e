note
	description: "Automated test harness for VoidRush Phase 3 TCP Echo and Phase 4 HTTP/1.1 Engine"

class
	VR_TEST_APP

inherit
	VR_REACTOR_CONSTANTS
	VR_SSE_CONSTANTS

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

			io.put_string ("[vr_test_app] Testing Phase 5 RFC 6455 crypto, framing, and unmasking...%N")
			test_ws_accept_key

			io.put_string ("[vr_test_app] Testing Phase 5 HTTP 101 WebSocket upgrade handshake and 400 rejection...%N")
			test_end_to_end_ws_upgrade

			io.put_string ("[vr_test_app] Testing Phase 5 WebSocket masked frame echo and unmasked rejection...%N")
			test_end_to_end_ws_echo_and_mask_rejection

			io.put_string ("[vr_test_app] Testing Phase 5 interleaved Ping control frame during message fragmentation...%N")
			test_end_to_end_ws_interleaved_ping

			io.put_string ("[vr_test_app] Testing Phase 5 multi-worker Pub/Sub room broadcast...%N")
			test_end_to_end_ws_pubsub_broadcast

			io.put_string ("[vr_test_app] Testing Phase 5 disconnect cleanup without ghost room deliveries...%N")
			test_end_to_end_ws_disconnect_cleanup

			io.put_string ("[vr_test_app] Testing Phase 5 WebSocket metrics and fast-path direct write...%N")
			test_end_to_end_ws_metrics

			io.put_string ("[vr_test_app] Testing Phase 6 SSE C codec primitives and ref-counted buffers...%N")
			test_sse_codec_primitives

			io.put_string ("[vr_test_app] Testing Phase 6 SSE handshake and retry directive...%N")
			test_end_to_end_sse_handshake

			io.put_string ("[vr_test_app] Testing Phase 6 HTTP POST /events broadcast to SSE subscribers...%N")
			test_end_to_end_sse_post_publish

			io.put_string ("[vr_test_app] Testing Phase 6 WebSocket to SSE cross-protocol bridge...%N")
			test_end_to_end_ws_sse_bridge

			io.put_string ("[vr_test_app] Testing Phase 6 unexpected inbound bytes discard on SSE stream...%N")
			test_end_to_end_sse_inbound_discard

			io.put_string ("[vr_test_app] Testing Phase 6 SSE client disconnect cleanup without ghost deliveries...%N")
			test_end_to_end_sse_disconnect_cleanup

			io.put_string ("[vr_test_app] All Eiffel Phase 3, Phase 4, Phase 5, and Phase 6 tests passed successfully!%N")
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

	test_ws_accept_key
			-- T5.1: Verify RFC 6455 accept key calculation, unmasking, and frame encoding via Eiffel FFI.
		local
			key_str: C_STRING
			accept_buf: MANAGED_POINTER
			len, i, hdr_len: INTEGER
			accept_str: STRING_8
			hdr_buf, data_buf, mask_buf: MANAGED_POINTER
		do
			create key_str.make ("dGhlIHNhbXBsZSBub25jZQ==")
			create accept_buf.make (32)
			len := {VR_REACTOR_BRIDGE}.c_ws_compute_accept_key (key_str.item, key_str.count, accept_buf.item)
			check accept_len_ok: len = 28 end
			create accept_str.make (28)
			from i := 0 until i >= 28 loop
				accept_str.append_character (accept_buf.read_character (i))
				i := i + 1
			end
			check accept_matches_vector: accept_str.same_string ("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") end

			-- Header encoding: 0-125 bytes
			create hdr_buf.make (16)
			hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | Ws_opcode_text).to_natural_8, 10, hdr_buf.item)
			check hdr_small_len: hdr_len = 2 end
			check hdr_b0: hdr_buf.read_natural_8 (0) = 0x81 end
			check hdr_b1: hdr_buf.read_natural_8 (1) = 10 end

			-- Header encoding: 126-65535 bytes
			hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | Ws_opcode_binary).to_natural_8, 1000, hdr_buf.item)
			check hdr_med_len: hdr_len = 4 end
			check hdr_med_b0: hdr_buf.read_natural_8 (0) = 0x82 end
			check hdr_med_b1: hdr_buf.read_natural_8 (1) = 126 end
			check hdr_med_payload: hdr_buf.read_natural_16_be (2) = 1000 end

			-- Header encoding: >65535 bytes
			hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | Ws_opcode_binary).to_natural_8, 70000, hdr_buf.item)
			check hdr_large_len: hdr_len = 10 end
			check hdr_large_b0: hdr_buf.read_natural_8 (0) = 0x82 end
			check hdr_large_b1: hdr_buf.read_natural_8 (1) = 127 end
			check hdr_large_payload: hdr_buf.read_natural_64_be (2) = (70000).to_natural_64 end

			-- In-place unmask test
			create data_buf.make (4)
			create mask_buf.make (4)
			mask_buf.put_natural_8 (0x12, 0)
			mask_buf.put_natural_8 (0x34, 1)
			mask_buf.put_natural_8 (0x56, 2)
			mask_buf.put_natural_8 (0x78, 3)
			data_buf.put_natural_8 (0x12, 0)
			data_buf.put_natural_8 (0x34, 1)
			data_buf.put_natural_8 (0x56, 2)
			data_buf.put_natural_8 (0x78, 3)
			{VR_REACTOR_BRIDGE}.c_ws_unmask (data_buf.item, 4, mask_buf.item)
			check unmasked_zero: data_buf.read_natural_8 (0) = 0 and data_buf.read_natural_8 (1) = 0 and data_buf.read_natural_8 (2) = 0 and data_buf.read_natural_8 (3) = 0 end
		end

	test_end_to_end_ws_upgrade
			-- T5.2: Verify HTTP 101 WebSocket upgrade handshake and 400 rejection.
		local
			r: separate VR_REACTOR
			c1, c2, res, i: INTEGER
			m: VR_METRICS
			resp1, resp2: STRING_8
		do
			create r.make (Test_port + 7, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			-- Client 1: valid upgrade
			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 7)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			from i := 0 until i >= 20 or (m.accepts_total >= 1 and m.active_connections >= 1) loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check c1_accepted: m.accepts_total >= 1 end

			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			resp1 := read_response_str (c1, r, 50)
			check got_101: resp1.has_substring ("HTTP/1.1 101 Switching Protocols") end
			check got_upgrade_hdr: resp1.has_substring ("Upgrade: websocket") end
			check got_conn_hdr: resp1.has_substring ("Connection: Upgrade") end
			check got_accept_hdr: resp1.has_substring ("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") end

			-- Client 2: invalid upgrade (bad version 12)
			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 7)
			check c2_connected: c2 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c2)

			from i := 0 until i >= 20 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_ws_upgrade (c2, "dGhlIHNhbXBsZSBub25jZQ==", "12")
			resp2 := read_response_str (c2, r, 20)
			check got_400: resp2.has_substring ("400 Bad Request") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			res := {VR_REACTOR_BRIDGE}.c_close (c2)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_echo_and_mask_rejection
			-- T5.3: Verify client masked frame echoes unmasked; unmasked frame triggers 1002 Close.
		local
			r: separate VR_REACTOR
			c1, res, i, n: INTEGER
			m: VR_METRICS
			resp_upgrade: STRING_8
			recv_buf: MANAGED_POINTER
			payload_out: STRING_8
		do
			create r.make (Test_port + 8, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 8)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			from i := 0 until i >= 20 or m.accepts_total >= 1 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			resp_upgrade := read_response_str (c1, r, 50)
			check upgrade_ok: resp_upgrade.has_substring ("101 Switching Protocols") end

			-- Send masked text frame: "Hello WebSocket!" (16 bytes)
			send_client_ws_frame (c1, True, Ws_opcode_text, "Hello WebSocket!", True)

			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 18)
			check echo_read: n >= 18 end
			check echo_b0: recv_buf.read_natural_8 (0) = 0x81 end
			check echo_b1: recv_buf.read_natural_8 (1) = 16 end
			create payload_out.make (16)
			from i := 0 until i >= 16 loop
				payload_out.append_character (recv_buf.read_character (2 + i))
				i := i + 1
			end
			check echo_payload_ok: payload_out.same_string ("Hello WebSocket!") end

			-- Send UNMASKED client frame: protocol error per RFC 6455 §5.1
			send_client_ws_frame (c1, True, Ws_opcode_text, "unmasked_error", False)

			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 4)
			check close_read: n >= 4 end
			check close_b0: recv_buf.read_natural_8 (0) = 0x88 end
			check close_b1: recv_buf.read_natural_8 (1) = 2 end
			check close_code_1002: recv_buf.read_natural_16_be (2) = Ws_close_protocol_error end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_interleaved_ping
			-- T5.4: Interleaved Ping during message fragmentation receives immediate Pong without corrupting frag_buf.
		local
			r: separate VR_REACTOR
			c1, res, i, n: INTEGER
			m: VR_METRICS
			resp_upgrade: STRING_8
			recv_buf: MANAGED_POINTER
			pong_payload, final_payload: STRING_8
		do
			create r.make (Test_port + 9, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 9)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			from i := 0 until i >= 20 or m.accepts_total >= 1 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			resp_upgrade := read_response_str (c1, r, 50)
			check upgrade_ok: resp_upgrade.has_substring ("101 Switching Protocols") end

			-- 1. Send fragment 1: FIN=0, text opcode, payload "Part1_"
			send_client_ws_frame (c1, False, Ws_opcode_text, "Part1_", True)

			-- 2. Interleave Ping: FIN=1, ping opcode, payload "myping"
			send_client_ws_frame (c1, True, Ws_opcode_ping, "myping", True)

			-- 3. Verify Pong received immediately
			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 8)
			check pong_read: n >= 8 end
			check pong_b0: recv_buf.read_natural_8 (0) = 0x8A end
			check pong_b1: recv_buf.read_natural_8 (1) = 6 end
			create pong_payload.make (6)
			from i := 0 until i >= 6 loop
				pong_payload.append_character (recv_buf.read_character (2 + i))
				i := i + 1
			end
			check pong_payload_ok: pong_payload.same_string ("myping") end

			-- 4. Send fragment 2: FIN=1, continuation opcode, payload "Part2_Done"
			send_client_ws_frame (c1, True, Ws_opcode_continuation, "Part2_Done", True)

			-- 5. Verify reassembled message echo: "Part1_Part2_Done" (16 bytes)
			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 18)
			check reassembled_read: n >= 18 end
			check echo_b0: recv_buf.read_natural_8 (0) = 0x81 end
			check echo_b1: recv_buf.read_natural_8 (1) = 16 end
			create final_payload.make (16)
			from i := 0 until i >= 16 loop
				final_payload.append_character (recv_buf.read_character (2 + i))
				i := i + 1
			end
			check reassembled_matches: final_payload.same_string ("Part1_Part2_Done") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_pubsub_broadcast
			-- T5.5: Multi-worker Pub/Sub broadcast via VR_BROKER with "PUB " command prefix.
		local
			r: separate VR_REACTOR
			c1, c2, res, i, n: INTEGER
			m: VR_METRICS
			recv_buf: MANAGED_POINTER
			broadcast_msg: STRING_8
		do
			create r.make (Test_port + 10, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 10)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 10)
			check c2_connected: c2 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c2)

			from i := 0 until i >= 30 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end
			check two_accepted: m.accepts_total >= 2 end

			-- Upgrade both to WebSocket
			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			send_ws_upgrade (c2, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			broadcast_msg := read_response_str (c1, r, 50)
			broadcast_msg := read_response_str (c2, r, 50)

			-- Client 1 publishes message: "PUB BroadcastToAll!" (19 bytes, payload without "PUB " is 15 bytes)
			send_client_ws_frame (c1, True, Ws_opcode_text, "PUB BroadcastToAll!", True)

			-- Client 2 must receive broadcast frame with "BroadcastToAll!" (15 bytes)
			create recv_buf.make (256)
			n := read_response_bytes (c2, r, recv_buf, 17)
			check c2_got_broadcast: n >= 17 end
			check c2_b0: recv_buf.read_natural_8 (0) = 0x81 end
			check c2_b1: recv_buf.read_natural_8 (1) = 15 end
			create broadcast_msg.make (15)
			from i := 0 until i >= 15 loop
				broadcast_msg.append_character (recv_buf.read_character (2 + i))
				i := i + 1
			end
			check payload_ok: broadcast_msg.same_string ("BroadcastToAll!") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			res := {VR_REACTOR_BRIDGE}.c_close (c2)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_disconnect_cleanup
			-- T5.6: Disconnect cleanup purges worker room tables immediately without ghost deliveries.
		local
			r: separate VR_REACTOR
			c1, c2, res, i, n: INTEGER
			m: VR_METRICS
			recv_buf: MANAGED_POINTER
			broadcast_msg: STRING_8
		do
			create r.make (Test_port + 11, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 11)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 11)
			check c2_connected: c2 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c2)

			from i := 0 until i >= 30 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			-- Upgrade both
			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			send_ws_upgrade (c2, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			broadcast_msg := read_response_str (c1, r, 50)
			broadcast_msg := read_response_str (c2, r, 50)

			-- Now client 2 abruptly disconnects
			res := {VR_REACTOR_BRIDGE}.c_close (c2)

			-- Step reactor so worker detects EOF and removes c2 from room lists
			from i := 0 until i >= 10 loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				i := i + 1
			end

			-- Client 1 publishes another message: "PUB StillHere!" (14 bytes, payload 10 bytes)
			send_client_ws_frame (c1, True, Ws_opcode_text, "PUB StillHere!", True)

			-- Client 1 receives the broadcast
			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 12)
			check c1_got_broadcast: n >= 12 end
			check c1_b0: recv_buf.read_natural_8 (0) = 0x81 end
			check c1_b1: recv_buf.read_natural_8 (1) = 10 end
			create broadcast_msg.make (10)
			from i := 0 until i >= 10 loop
				broadcast_msg.append_character (recv_buf.read_character (2 + i))
				i := i + 1
			end
			check payload_ok: broadcast_msg.same_string ("StillHere!") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_metrics
			-- T5.7: Verify atomic WebSocket metrics tracking and direct write fast path.
		local
			r: separate VR_REACTOR
			c1, res, i: INTEGER
			m: VR_METRICS
			recv_buf: MANAGED_POINTER
			resp_upgrade: STRING_8
			n: INTEGER
		do
			create r.make (Test_port + 12, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 12)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			from i := 0 until i >= 20 or m.accepts_total >= 1 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_ws_upgrade (c1, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			resp_upgrade := read_response_str (c1, r, 50)
			check upgrade_ok: resp_upgrade.has_substring ("101 Switching Protocols") end

			send_client_ws_frame (c1, True, Ws_opcode_text, "MetricCheck", True)
			create recv_buf.make (256)
			n := read_response_bytes (c1, r, recv_buf, 13)
			check echo_read: n >= 13 end

			m.refresh (get_native_handle (r))
			check ws_upgrades_tracked: m.ws_upgrades_total >= 1 end
			check ws_in_tracked: m.ws_frames_in_total >= 1 end
			check ws_out_tracked: m.ws_frames_out_total >= 1 end
			check ws_no_dropped: m.ws_dropped_backpressure_total = 0 end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

feature {NONE} -- WebSocket Test Helpers

	send_ws_upgrade (a_sock: INTEGER; a_key: STRING_8; a_version: STRING_8)
			-- Send HTTP/1.1 WebSocket Upgrade request.
		local
			req: STRING_8
			cs: C_STRING
			res: INTEGER
		do
			create req.make (256)
			req.append ("GET /ws HTTP/1.1%R%NHost: localhost%R%NUpgrade: websocket%R%NConnection: Upgrade%R%N")
			if not a_key.is_empty then
				req.append ("Sec-WebSocket-Key: ")
				req.append (a_key)
				req.append ("%R%N")
			end
			if not a_version.is_empty then
				req.append ("Sec-WebSocket-Version: ")
				req.append (a_version)
				req.append ("%R%N")
			end
			req.append ("%R%N")
			create cs.make (req)
			res := {VR_REACTOR_BRIDGE}.c_send (a_sock, cs.item, cs.count)
			check sent_upgrade: res = cs.count end
		end

	send_client_ws_frame (a_sock: INTEGER; a_fin: BOOLEAN; a_opcode: INTEGER; a_payload: STRING_8; a_masked: BOOLEAN)
			-- Transmit client frame with or without masking.
		local
			buf: MANAGED_POINTER
			b0, b1: NATURAL_8
			total_len, i, res: INTEGER
		do
			b0 := (if a_fin then 0x80 else 0 end | a_opcode).to_natural_8
			if a_masked then
				b1 := (0x80 | a_payload.count).to_natural_8
				total_len := 2 + 4 + a_payload.count
				create buf.make (total_len)
				buf.put_natural_8 (b0, 0)
				buf.put_natural_8 (b1, 1)
				buf.put_natural_8 (0x12, 2)
				buf.put_natural_8 (0x34, 3)
				buf.put_natural_8 (0x56, 4)
				buf.put_natural_8 (0x78, 5)

				from i := 1 until i > a_payload.count loop
					buf.put_character (a_payload.item (i), 6 + i - 1)
					i := i + 1
				end

				if a_payload.count > 0 then
					{VR_REACTOR_BRIDGE}.c_ws_unmask (buf.item + 6, a_payload.count, buf.item + 2)
				end
			else
				b1 := a_payload.count.to_natural_8
				total_len := 2 + a_payload.count
				create buf.make (total_len)
				buf.put_natural_8 (b0, 0)
				buf.put_natural_8 (b1, 1)
				from i := 1 until i > a_payload.count loop
					buf.put_character (a_payload.item (i), 2 + i - 1)
					i := i + 1
				end
			end
			res := {VR_REACTOR_BRIDGE}.c_send (a_sock, buf.item, total_len)
			check sent_frame_ok: res = total_len end
		end

	read_response_str (a_sock: INTEGER; r: separate VR_REACTOR; a_min_bytes: INTEGER): STRING_8
			-- Step reactor and drain up to a_min_bytes from a_sock, returning string.
		local
			recv_buf: MANAGED_POINTER
			i, n, total_read: INTEGER
		do
			create recv_buf.make (2048)
			from
				i := 0
				total_read := 0
			until
				i >= 40 or total_read >= a_min_bytes
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (a_sock, recv_buf.item + total_read, 2048 - total_read)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end
			if total_read > 0 then
				recv_buf.put_integer_8 (0, total_read)
				Result := (create {C_STRING}.make_by_pointer (recv_buf.item)).string
			else
				create Result.make_empty
			end
		end

	read_response_bytes (a_sock: INTEGER; r: separate VR_REACTOR; a_buf: MANAGED_POINTER; a_min_bytes: INTEGER): INTEGER
			-- Step reactor and drain into a_buf, returning total bytes read.
		local
			i, n, total_read: INTEGER
		do
			from
				i := 0
				total_read := 0
			until
				i >= 40 or total_read >= a_min_bytes
			loop
				step_reactor (r, 50)
				{VR_REACTOR_BRIDGE}.c_sleep_ms (10)
				n := {VR_REACTOR_BRIDGE}.c_reactor_read (a_sock, a_buf.item + total_read, a_buf.count - total_read)
				if n > 0 then
					total_read := total_read + n
				end
				i := i + 1
			end
			Result := total_read
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

feature {NONE} -- Phase 6 SSE Tests & Helpers

	test_sse_codec_primitives
			-- T6.1: Verify C SSE formatting (single/multi-line, CRLF stripping) and ref-counted buffers.
		local
			buf: MANAGED_POINTER
			id_str, ev_str, dt_str: STRING_8
			c_id, c_ev, c_dt: ANY
			n: INTEGER
			out_str: STRING_8
			msg: POINTER
			data_ptr: POINTER
			sz: INTEGER
		do
			create buf.make (512)

			-- 1a. Single-line with id & event
			id_str := "42"
			ev_str := "greeting"
			dt_str := "hello world"
			c_id := id_str.to_c
			c_ev := ev_str.to_c
			c_dt := dt_str.to_c
			n := {VR_REACTOR_BRIDGE}.c_sse_format_event ($c_id, id_str.count, $c_ev, ev_str.count, $c_dt, dt_str.count, buf.item, buf.count)
			check sse_single_line_ok: n > 0 end
			buf.put_integer_8 (0, n)
			out_str := (create {C_STRING}.make_by_pointer (buf.item)).string
			check sse_single_line_str: out_str.same_string ("id: 42%Nevent: greeting%Ndata: hello world%N%N") end

			-- 1b. Multi-line with \n
			dt_str := "line1%Nline2%Nline3"
			c_dt := dt_str.to_c
			n := {VR_REACTOR_BRIDGE}.c_sse_format_event (default_pointer, 0, default_pointer, 0, $c_dt, dt_str.count, buf.item, buf.count)
			check sse_multi_line_ok: n > 0 end
			buf.put_integer_8 (0, n)
			out_str := (create {C_STRING}.make_by_pointer (buf.item)).string
			check sse_multi_line_str: out_str.same_string ("data: line1%Ndata: line2%Ndata: line3%N%N") end

			-- 1c. CRLF injection stripping in id and event
			id_str := "id%R%N1"
			ev_str := "ev%Nent"
			dt_str := "safe"
			c_id := id_str.to_c
			c_ev := ev_str.to_c
			c_dt := dt_str.to_c
			n := {VR_REACTOR_BRIDGE}.c_sse_format_event ($c_id, id_str.count, $c_ev, ev_str.count, $c_dt, dt_str.count, buf.item, buf.count)
			check sse_crlf_strip_ok: n > 0 end
			buf.put_integer_8 (0, n)
			out_str := (create {C_STRING}.make_by_pointer (buf.item)).string
			check sse_crlf_stripped: out_str.same_string ("id: id1%Nevent: event%Ndata: safe%N%N") end

			-- 1d. Ref-counted message creation and unref
			id_str := "100"
			ev_str := "msg"
			dt_str := "payload"
			c_id := id_str.to_c
			c_ev := ev_str.to_c
			c_dt := dt_str.to_c
			msg := {VR_REACTOR_BRIDGE}.c_sse_msg_create ($c_id, id_str.count, $c_ev, ev_str.count, $c_dt, dt_str.count, 2)
			check sse_msg_created: msg /= default_pointer end
			sz := {VR_REACTOR_BRIDGE}.c_ws_msg_size (msg)
			check sse_msg_sz: sz > 0 end
			data_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_data (msg)
			check sse_data_ptr: data_ptr /= default_pointer end
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (msg)
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (msg)
		end

	test_end_to_end_sse_handshake
			-- T6.2: Verify GET /events returns 200 OK text/event-stream headers and retry directive.
		local
			r: separate VR_REACTOR
			c1, res, i: INTEGER
			m: VR_METRICS
			resp: STRING_8
		do
			create r.make (Test_port + 13, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 13)
			check c1_connected: c1 >= 0 end
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)

			from i := 0 until i >= 20 or m.accepts_total >= 1 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_sse_request (c1, "99")
			resp := read_response_str (c1, r, 50)
			check sse_resp_200: resp.has_substring ("HTTP/1.1 200 OK") end
			check sse_resp_ct: resp.has_substring ("Content-Type: text/event-stream") end
			check sse_resp_cc: resp.has_substring ("Cache-Control: no-cache") end
			check sse_resp_conn: resp.has_substring ("Connection: keep-alive") end
			check sse_resp_accel: resp.has_substring ("X-Accel-Buffering: no") end
			check sse_resp_retry: resp.has_substring ("retry: 3000") end

			m.refresh (get_native_handle (r))
			check sse_stream_counted: m.sse_streams_total >= 1 end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			shutdown_reactor (r)
		end

	test_end_to_end_sse_post_publish
			-- T6.4: Connect 2 SSE clients, publish via HTTP POST /events, verify both receive broadcast.
		local
			r: separate VR_REACTOR
			c1, c2, pub, res, i: INTEGER
			m: VR_METRICS
			resp1, resp2, resp_pub, evt1, evt2: STRING_8
		do
			create r.make (Test_port + 14, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 14)
			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 14)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c2)

			from i := 0 until i >= 20 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_sse_request (c1, "")
			send_sse_request (c2, "")
			resp1 := read_response_str (c1, r, 50)
			resp2 := read_response_str (c2, r, 50)
			check c1_open: resp1.has_substring ("200 OK") end
			check c2_open: resp2.has_substring ("200 OK") end

			-- Publisher sends POST /events
			pub := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 14)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (pub)
			from i := 0 until i >= 20 or m.accepts_total >= 3 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_http_post (pub, "/events", "Hello SSE Broadcast")
			resp_pub := read_response_str (pub, r, 30)
			check pub_accepted: resp_pub.has_substring ("202 Accepted") end

			evt1 := read_response_str (c1, r, 20)
			evt2 := read_response_str (c2, r, 20)
			check evt1_ok: evt1.has_substring ("data: Hello SSE Broadcast") end
			check evt2_ok: evt2.has_substring ("data: Hello SSE Broadcast") end

			m.refresh (get_native_handle (r))
			check sse_events_counted: m.sse_events_out_total >= 2 end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			res := {VR_REACTOR_BRIDGE}.c_close (c2)
			res := {VR_REACTOR_BRIDGE}.c_close (pub)
			shutdown_reactor (r)
		end

	test_end_to_end_ws_sse_bridge
			-- T6.4b: Cross-protocol publishing from WebSocket (PUB_SSE) to SSE subscribers.
		local
			r: separate VR_REACTOR
			c_sse, c_ws, res, i: INTEGER
			m: VR_METRICS
			resp_sse, resp_ws, evt: STRING_8
		do
			create r.make (Test_port + 15, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c_sse := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 15)
			c_ws := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 15)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c_sse)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c_ws)

			from i := 0 until i >= 20 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			-- SSE client connects
			send_sse_request (c_sse, "")
			resp_sse := read_response_str (c_sse, r, 50)
			check sse_ok: resp_sse.has_substring ("200 OK") end

			-- WS client upgrades
			send_ws_upgrade (c_ws, "dGhlIHNhbXBsZSBub25jZQ==", "13")
			resp_ws := read_response_str (c_ws, r, 50)
			check ws_upgraded: resp_ws.has_substring ("101 Switching Protocols") end

			-- WS client publishes to SSE via PUB_SSE
			send_client_ws_frame (c_ws, True, Ws_opcode_text, "PUB_SSE CrossProtocolBridge", True)

			evt := read_response_str (c_sse, r, 20)
			check bridge_evt_ok: evt.has_substring ("data: CrossProtocolBridge") end

			res := {VR_REACTOR_BRIDGE}.c_close (c_sse)
			res := {VR_REACTOR_BRIDGE}.c_close (c_ws)
			shutdown_reactor (r)
		end

	test_end_to_end_sse_inbound_discard
			-- T6.6: Verify unexpected inbound bytes on active SSE connection are discarded without breaking stream.
		local
			r: separate VR_REACTOR
			c_sse, pub, res, i: INTEGER
			m: VR_METRICS
			resp_sse, resp_pub, evt: STRING_8
			junk: STRING_8
			cs: C_STRING
		do
			create r.make (Test_port + 16, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c_sse := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 16)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c_sse)

			from i := 0 until i >= 20 or m.accepts_total >= 1 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_sse_request (c_sse, "")
			resp_sse := read_response_str (c_sse, r, 50)
			check sse_open: resp_sse.has_substring ("200 OK") end

			-- Client sends unexpected inbound data
			junk := "MALICIOUS_OR_BUGGY_INBOUND_DATA_THAT_MUST_BE_DISCARDED"
			create cs.make (junk)
			res := {VR_REACTOR_BRIDGE}.c_send (c_sse, cs.item, cs.count)
			check sent_junk: res = cs.count end

			step_reactor (r, 50)
			step_reactor (r, 50)

			-- Now publish an event via POST /events to verify stream is still operational
			pub := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 16)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (pub)
			from i := 0 until i >= 20 or m.accepts_total >= 2 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_http_post (pub, "/events", "StillAliveAfterDiscard")
			resp_pub := read_response_str (pub, r, 30)
			check pub_ok: resp_pub.has_substring ("202 Accepted") end

			evt := read_response_str (c_sse, r, 20)
			check sse_alive: evt.has_substring ("data: StillAliveAfterDiscard") end

			res := {VR_REACTOR_BRIDGE}.c_close (c_sse)
			res := {VR_REACTOR_BRIDGE}.c_close (pub)
			shutdown_reactor (r)
		end

	test_end_to_end_sse_disconnect_cleanup
			-- T6.5: Verify abrupt client disconnect removes from sse_rooms without ghost deliveries.
		local
			r: separate VR_REACTOR
			c1, c2, c3, pub, res, i: INTEGER
			m: VR_METRICS
			resp1, resp2, resp3, resp_pub, evt1, evt3: STRING_8
		do
			create r.make (Test_port + 17, 4)
			check listening: is_reactor_listening (r) end
			create m.make_from_pointer (get_native_handle (r))

			c1 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 17)
			c2 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 17)
			c3 := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 17)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c1)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c2)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (c3)

			from i := 0 until i >= 20 or m.accepts_total >= 3 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_sse_request (c1, "")
			send_sse_request (c2, "")
			send_sse_request (c3, "")
			resp1 := read_response_str (c1, r, 50)
			resp2 := read_response_str (c2, r, 50)
			resp3 := read_response_str (c3, r, 50)
			check c1_open: resp1.has_substring ("200 OK") end
			check c2_open: resp2.has_substring ("200 OK") end
			check c3_open: resp3.has_substring ("200 OK") end

			-- Abruptly close c2
			res := {VR_REACTOR_BRIDGE}.c_close (c2)
			step_reactor (r, 50)
			step_reactor (r, 50)

			-- Publish event to room
			pub := {VR_REACTOR_BRIDGE}.c_connect_tcp (default_pointer, Test_port + 17)
			res := {VR_REACTOR_BRIDGE}.c_set_nonblocking (pub)
			from i := 0 until i >= 20 or m.accepts_total >= 4 loop
				step_reactor (r, 50)
				m.refresh (get_native_handle (r))
				i := i + 1
			end

			send_http_post (pub, "/events", "SurvivingClientsOnly")
			resp_pub := read_response_str (pub, r, 30)
			check pub_ok: resp_pub.has_substring ("202 Accepted") end

			evt1 := read_response_str (c1, r, 20)
			evt3 := read_response_str (c3, r, 20)
			check evt1_ok: evt1.has_substring ("data: SurvivingClientsOnly") end
			check evt3_ok: evt3.has_substring ("data: SurvivingClientsOnly") end

			res := {VR_REACTOR_BRIDGE}.c_close (c1)
			res := {VR_REACTOR_BRIDGE}.c_close (c3)
			res := {VR_REACTOR_BRIDGE}.c_close (pub)
			shutdown_reactor (r)
		end

	send_sse_request (a_sock: INTEGER; a_last_id: STRING_8)
			-- Send HTTP/1.1 SSE request.
		local
			req: STRING_8
			cs: C_STRING
			res: INTEGER
		do
			create req.make (256)
			req.append ("GET /events HTTP/1.1%R%NHost: localhost%R%NAccept: text/event-stream%R%N")
			if not a_last_id.is_empty then
				req.append ("Last-Event-ID: " + a_last_id + "%R%N")
			end
			req.append ("%R%N")
			create cs.make (req)
			res := {VR_REACTOR_BRIDGE}.c_send (a_sock, cs.item, cs.count)
			check sent_sse_req: res = cs.count end
		end

	send_http_post (a_sock: INTEGER; a_path, a_body: STRING_8)
			-- Send HTTP/1.1 POST request with body.
		local
			req: STRING_8
			cs: C_STRING
			res: INTEGER
		do
			create req.make (256 + a_body.count)
			req.append ("POST " + a_path + " HTTP/1.1%R%NHost: localhost%R%NContent-Length: " + a_body.count.out + "%R%N%R%N" + a_body)
			create cs.make (req)
			res := {VR_REACTOR_BRIDGE}.c_send (a_sock, cs.item, cs.count)
			check sent_post_req: res = cs.count end
		end

feature {NONE} -- Constants

	Test_port: INTEGER = 19877
			-- Dedicated test port base

end
