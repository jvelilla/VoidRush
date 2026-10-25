note
	description: "Non-separate TCP connection state with dual in/out buffers and incremental HTTP/1.1 pipeline"

class
	VR_CONNECTION

inherit
	VR_REACTOR_CONSTANTS
	VR_SSE_CONSTANTS

create
	make,
	make_with_time,
	make_with_proto

feature {NONE} -- Initialization

	make (a_fd: INTEGER; a_generation: NATURAL_32)
			-- Initialize connection for socket `a_fd` protected by `a_generation`.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		do
			make_with_time (a_fd, a_generation, {VR_REACTOR_BRIDGE}.c_reactor_now_ms)
		ensure
			fd_set: fd = a_fd
			generation_set: generation = a_generation
			is_open: is_open
			not_paused: not paused_in
			out_buffer_empty: out_buf_bytes = 0
		end

	make_with_time (a_fd: INTEGER; a_generation: NATURAL_32; a_now_ms: NATURAL_64)
			-- Initialize connection for socket `a_fd` protected by `a_generation` at timestamp `a_now_ms`.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		do
			make_with_proto (a_fd, a_generation, a_now_ms, Proto_http_reading_headers)
		ensure
			fd_set: fd = a_fd
			generation_set: generation = a_generation
			is_open: is_open
			not_paused: not paused_in
			out_buffer_empty: out_buf_bytes = 0
		end

	make_with_proto (a_fd: INTEGER; a_generation: NATURAL_32; a_now_ms: NATURAL_64; a_proto: INTEGER)
			-- Initialize connection with explicit protocol mode `a_proto`.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		do
			fd := a_fd
			generation := a_generation
			state := State_open
			proto_state := a_proto
			last_active_ms := a_now_ms
			request_started_ms := 0
			paused_in := False

			create in_buf.make (Scratch_buffer_size)
			in_read_offset := 0
			in_write_offset := 0

			create out_buf.make (Scratch_buffer_size)
			out_read_offset := 0
			out_write_offset := 0

			create http_parser.make
			create proxy_parser.make
			create ws_parser.make
			create rooms.make (4)
			frag_bytes := 0
			frag_opcode := 0
			client_ip := "127.0.0.1"
			client_port := 0
			last_sse_heartbeat_ms := 0
			create last_sse_event_id.make_empty
		ensure
			fd_set: fd = a_fd
			generation_set: generation = a_generation
			proto_set: proto_state = a_proto
			is_open: is_open
			not_paused: not paused_in
			in_buffer_empty: in_buf_bytes = 0
			out_buffer_empty: out_buf_bytes = 0
		end

feature -- Access

	fd: INTEGER
			-- Socket file descriptor

	generation: NATURAL_32
			-- Monotonic generation token assigned at accept

	state: INTEGER
			-- Lifecycle state (State_open, State_closing, State_closed)

	proto_state: INTEGER
			-- Current protocol sub-state

	last_active_ms: NATURAL_64
			-- Last activity timestamp in monotonic milliseconds

	request_started_ms: NATURAL_64
			-- Timestamp when the first byte of the current request arrived (for Slowloris defense)

	paused_in: BOOLEAN
			-- True when flow control has paused Reactor_in interest

	client_ip: STRING_8
			-- IP address of remote peer (or extracted from PROXY v2 header)

	client_port: INTEGER
			-- Port number of remote peer (or extracted from PROXY v2 header)

	http_parser: VR_HTTP_PARSER
			-- Zero-allocation HTTP/1.1 incremental request parser

	proxy_parser: VR_PROXY_V2
			-- Binary PROXY Protocol v2 preface parser

	ws_parser: VR_WS_PARSER
			-- Incremental zero-allocation RFC 6455 WebSocket frame parser

	frag_buf: detachable MANAGED_POINTER
			-- Lazy accumulation buffer for fragmented WebSocket messages

	frag_bytes: INTEGER
			-- Accumulated byte count in frag_buf

	frag_opcode: INTEGER
			-- In-progress fragmented data frame opcode (0x1 text or 0x2 binary)

	rooms: ARRAYED_SET [INTEGER]
			-- Room subscriptions for this connection

	broadcast_handler: detachable PROCEDURE [INTEGER, INTEGER, POINTER, INTEGER]
			-- Callback into worker to broadcast message across workers (room, opcode, ptr, len)

	room_join_handler: detachable PROCEDURE [INTEGER, INTEGER]
			-- Callback into worker when socket joins a room (fd, room)

	last_sse_heartbeat_ms: NATURAL_64
			-- Monotonic timestamp when the last heartbeat was successfully written

	last_sse_event_id: STRING_8
			-- Last processed or received event ID for reconnection resumption

	sse_broadcast_handler: detachable PROCEDURE [INTEGER, POINTER, INTEGER]
			-- Callback into worker to broadcast SSE message across workers (room, ptr, len)

	sse_room_join_handler: detachable PROCEDURE [INTEGER, INTEGER]
			-- Callback into worker when socket joins an SSE room (fd, room)

	reactor_handle: POINTER
			-- Native reactor handle for atomic metrics updates

	in_buf_bytes: INTEGER
			-- Number of unparsed bytes sitting in inbound buffer
		do
			Result := in_write_offset - in_read_offset
		ensure
			non_negative: Result >= 0
		end

	out_buf_bytes: INTEGER
			-- Number of unwritten bytes in outbound buffer
		do
			Result := out_write_offset - out_read_offset
		ensure
			non_negative: Result >= 0
		end

	read_pointer: POINTER
			-- Direct memory pointer to unwritten bytes in outbound buffer
		require
			has_bytes: out_buf_bytes > 0
		do
			Result := out_buf.item + out_read_offset
		ensure
			valid_pointer: Result /= default_pointer
		end

feature -- Status Report

	matches (a_generation: NATURAL_32): BOOLEAN
			-- Does incoming event generation match active/closing connection?
		do
			Result := (generation = a_generation) and (state /= State_closed)
		end

	is_open: BOOLEAN
			-- Is connection active?
		do
			Result := state = State_open
		end

	is_ws_open: BOOLEAN
			-- Is connection in active WebSocket full-duplex framing mode?
		do
			Result := (state = State_open) and (proto_state = Proto_ws_open)
		end

	is_closing: BOOLEAN
			-- Is connection draining remaining outbound writes before closure?
		do
			Result := state = State_closing
		end

	is_closed: BOOLEAN
			-- Has connection transitioned to closed?
		do
			Result := state = State_closed
		end

	is_in_room (a_room_id: INTEGER): BOOLEAN
			-- Is connection registered in room `a_room_id`?
		do
			Result := rooms.has (a_room_id)
		end

	is_idle (a_now_ms: NATURAL_64; a_timeout_ms: NATURAL_64): BOOLEAN
			-- Has connection exceeded inactivity threshold?
		do
			Result := (a_now_ms > last_active_ms) and then ((a_now_ms - last_active_ms) >= a_timeout_ms)
		end

	is_expired (a_now_ms: NATURAL_64): BOOLEAN
			-- Evaluates dual timeouts: Slowloris request deadline OR keep-alive idle timeout
		do
			if request_started_ms > 0 then
				-- Actively accumulating request headers or body: apply strict Slowloris timeout
				Result := (a_now_ms > request_started_ms) and then ((a_now_ms - request_started_ms) >= Request_timeout_ms)
			else
				-- Waiting for next keep-alive request: apply standard idle timeout
				Result := (a_now_ms > last_active_ms) and then ((a_now_ms - last_active_ms) >= Idle_timeout_ms)
			end
		end

	interest_flags: NATURAL_32
			-- Pure function of connection state and flow control policy.
		local
			want_in, want_out: BOOLEAN
		do
			want_in := (state = State_open) and (not paused_in)
			want_out := out_buf_bytes > 0
			Result := 0
			if want_in then
				Result := Result | Reactor_in
			end
			if want_out then
				Result := Result | Reactor_out
			end
		end

feature -- State Modifiers

	set_proto_state (a_proto: INTEGER)
			-- Change protocol sub-state.
		do
			proto_state := a_proto
		ensure
			proto_set: proto_state = a_proto
		end

	register_room (a_room_id: INTEGER)
			-- Record that this connection joined `a_room_id`.
		require
			valid_room: a_room_id >= 0
		do
			rooms.extend (a_room_id)
		ensure
			registered: rooms.has (a_room_id)
		end

	unregister_room (a_room_id: INTEGER)
			-- Record that this connection left `a_room_id`.
		require
			valid_room: a_room_id >= 0
		do
			rooms.prune (a_room_id)
		ensure
			unregistered: not rooms.has (a_room_id)
		end

	set_broadcast_handler (a_handler: PROCEDURE [INTEGER, INTEGER, POINTER, INTEGER])
			-- Set worker callback for fanning out broadcasts.
		require
			valid_handler: a_handler /= Void
		do
			broadcast_handler := a_handler
		ensure
			handler_set: broadcast_handler = a_handler
		end

	set_room_join_handler (a_handler: PROCEDURE [INTEGER, INTEGER])
			-- Set worker callback for joining rooms.
		require
			valid_handler: a_handler /= Void
		do
			room_join_handler := a_handler
		ensure
			handler_set: room_join_handler = a_handler
		end

	set_sse_broadcast_handler (a_handler: PROCEDURE [INTEGER, POINTER, INTEGER])
			-- Set worker callback for SSE broadcast publishing.
		require
			valid_handler: a_handler /= Void
		do
			sse_broadcast_handler := a_handler
		ensure
			handler_set: sse_broadcast_handler = a_handler
		end

	set_sse_room_join_handler (a_handler: PROCEDURE [INTEGER, INTEGER])
			-- Set worker callback for SSE room joining.
		require
			valid_handler: a_handler /= Void
		do
			sse_room_join_handler := a_handler
		ensure
			handler_set: sse_room_join_handler = a_handler
		end

	set_last_sse_heartbeat_ms (a_time: NATURAL_64)
			-- Update heartbeat timestamp after a successful heartbeat write.
		require
			valid_time: a_time > 0
		do
			last_sse_heartbeat_ms := a_time
		ensure
			time_set: last_sse_heartbeat_ms = a_time
		end

	start_sse (a_last_event_id: detachable STRING_8; a_now_ms: NATURAL_64)
			-- Transition connection to Proto_sse_open and queue 200 OK headers.
		require
			is_open: is_open
			valid_time: a_now_ms > 0
		do
			proto_state := Proto_sse_open
			last_sse_heartbeat_ms := a_now_ms
			if attached a_last_event_id as lid then
				last_sse_event_id := lid.twin
			else
				create last_sse_event_id.make_empty
			end
			append_outbound_string (Sse_handshake_headers)
			if attached sse_room_join_handler as sjh then
				sjh.call ([fd, Room_default])
			end
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_metric_inc_sse_streams (reactor_handle)
			end
		ensure
			state_set: proto_state = Proto_sse_open
			heartbeat_initialized: last_sse_heartbeat_ms = a_now_ms
			out_buf_has_headers: out_buf_bytes >= Sse_handshake_headers.count
		end

	set_reactor_handle (a_handle: POINTER)
			-- Associate native reactor handle for metrics tracking.
		do
			reactor_handle := a_handle
		ensure
			handle_set: reactor_handle = a_handle
		end

	touch (a_now_ms: NATURAL_64)
			-- Update last activity timestamp.
		require
			valid_time: a_now_ms >= last_active_ms
		do
			last_active_ms := a_now_ms
		ensure
			updated: last_active_ms = a_now_ms
		end

	set_paused_in (a_paused: BOOLEAN)
			-- Set or clear inbound pause latch.
		do
			paused_in := a_paused
		ensure
			paused_set: paused_in = a_paused
		end

	mark_closing
			-- Mark connection as closing down.
		require
			is_open: is_open
		do
			state := State_closing
		ensure
			is_closing: state = State_closing
		end

	mark_closed
			-- Finalize connection closure.
		require
			not_closed: not is_closed
		do
			state := State_closed
		ensure
			is_closed: is_closed
		end

feature -- Data Ingestion Pipeline (Phase 4 Zero-Allocation Hook)

	on_data_received (a_src: POINTER; a_len: INTEGER; a_now_ms: NATURAL_64)
			-- Ingest incoming chunk from worker scratch buffer without heap allocations.
		require
			valid_src: a_src /= default_pointer
			valid_len: a_len > 0
		do
			touch (a_now_ms)
			if proto_state = Proto_echo then
				append_echo (a_src, a_len)
			else
				if request_started_ms = 0 and in_buf_bytes = 0 then
					request_started_ms := a_now_ms
				end
				append_inbound (a_src, a_len)
				process_inbound_pipeline (a_now_ms)
			end
		end

feature {NONE} -- Inbound Protocol Pipeline

	process_inbound_pipeline (a_now_ms: NATURAL_64)
			-- Drive state machine over bytes accumulated in `in_buf`.
		local
			done: BOOLEAN
		do
			from
				done := False
			until
				done or in_buf_bytes = 0 or state /= State_open
			loop
				if proto_state = Proto_awaiting_proxy_v2 then
					if proxy_parser.try_parse (in_buf, in_read_offset, in_write_offset) then
						in_read_offset := in_read_offset + proxy_parser.header_bytes_consumed
						if in_read_offset = in_write_offset then
							in_read_offset := 0
							in_write_offset := 0
						end
						client_ip := proxy_parser.client_ip
						client_port := proxy_parser.client_port
						proto_state := Proto_http_reading_headers
					else
						done := True
					end
				elseif proto_state = Proto_http_reading_headers then
					if http_parser.try_parse_request_header (in_buf, in_read_offset, in_write_offset) then
						in_read_offset := in_read_offset + http_parser.header_bytes_consumed
						if in_read_offset = in_write_offset then
							in_read_offset := 0
							in_write_offset := 0
						end

						if http_parser.is_upgrade_websocket or http_parser.is_connection_upgrade then
							process_websocket_upgrade (a_now_ms)
							if proto_state = Proto_ws_open and in_buf_bytes > 0 then
								-- CRITICAL TRAP D FIX: Process pipelined WebSocket frames immediately!
								process_websocket_frames (a_now_ms)
							end
							done := (proto_state /= Proto_ws_open)
						elseif http_parser.is_sse_request (in_buf) then
							process_sse_handshake (a_now_ms)
							done := True
						elseif http_parser.is_sse_publish_request (in_buf) then
							if http_parser.content_length > 0 then
								proto_state := Proto_http_reading_body
								if in_buf_bytes >= http_parser.content_length then
									process_http_body (a_now_ms)
								else
									done := True
								end
							else
								append_outbound_string (Sse_accepted_post_response)
								if not http_parser.is_keep_alive then
									mark_closing
									done := True
								else
									http_parser.reset
								end
							end
						else
							handle_http_request (a_now_ms)

							if not http_parser.is_keep_alive then
								mark_closing
								done := True
							else
								-- Reset Slowloris deadline for next pipelined request
								if in_buf_bytes = 0 then
									request_started_ms := 0
								else
									request_started_ms := a_now_ms
								end
								http_parser.reset
							end
						end
					else
						-- Headers not yet complete
						if in_buf_bytes >= Max_header_size then
							append_error_response (431, "Request Header Fields Too Large")
							mark_closing
						end
						done := True
					end
				elseif proto_state = Proto_http_reading_body then
					if in_buf_bytes >= http_parser.content_length then
						process_http_body (a_now_ms)
					else
						done := True
					end
				elseif proto_state = Proto_ws_open then
					process_websocket_frames (a_now_ms)
					done := True
				elseif proto_state = Proto_sse_open then
					-- SSE is strictly server-to-client; drain and discard unexpected inbound data
					in_read_offset := 0
					in_write_offset := 0
					done := True
				else
					done := True
				end
			end
		end

	process_sse_handshake (a_now_ms: NATURAL_64)
			-- Establish WHATWG Server-Sent Events stream.
		local
			lid: STRING_8
		do
			if not http_parser.last_event_id_slice.is_empty then
				lid := http_parser.last_event_id_slice.to_string_8 (in_buf)
			else
				create lid.make_empty
			end
			start_sse (lid, a_now_ms)
		end

	process_http_body (a_now_ms: NATURAL_64)
			-- Process request body for POST endpoints (e.g. POST /events publish).
		local
			body_ptr: POINTER
			body_len: INTEGER
		do
			body_len := http_parser.content_length
			body_ptr := in_buf.item + in_read_offset
			in_read_offset := in_read_offset + body_len
			if in_read_offset = in_write_offset then
				in_read_offset := 0
				in_write_offset := 0
			end

			if attached sse_broadcast_handler as sbh then
				sbh.call ([Room_default, body_ptr, body_len])
			end

			append_outbound_string (Sse_accepted_post_response)
			if not http_parser.is_keep_alive then
				mark_closing
			else
				proto_state := Proto_http_reading_headers
				if in_buf_bytes = 0 then
					request_started_ms := 0
				else
					request_started_ms := a_now_ms
				end
				http_parser.reset
			end
		end

	process_websocket_upgrade (a_now_ms: NATURAL_64)
			-- Validate WebSocket upgrade request, compute accept key, and transition to Proto_ws_open.
		local
			accept_buf: MANAGED_POINTER
			res, i, key_offset: INTEGER
			resp: STRING_8
		do
			if http_parser.is_websocket_upgrade_request then
				create accept_buf.make (32)
				key_offset := http_parser.ws_key_slice.offset
				res := {VR_REACTOR_BRIDGE}.c_ws_compute_accept_key (in_buf.item + key_offset, 24, accept_buf.item)
				if res = 28 then
					create resp.make (160)
					resp.append ("HTTP/1.1 101 Switching Protocols%R%NUpgrade: websocket%R%NConnection: Upgrade%R%NSec-WebSocket-Accept: ")
					from
						i := 0
					until
						i >= 28
					loop
						resp.append_character (accept_buf.read_character (i))
						i := i + 1
					end
					resp.append ("%R%N%R%N")
					append_outbound_string (resp)

					proto_state := Proto_ws_open
					request_started_ms := 0
					rooms.extend (Room_default)
					if attached room_join_handler as h then
						h.call ([fd, Room_default])
					end
					if reactor_handle /= default_pointer then
						{VR_REACTOR_BRIDGE}.c_metric_inc_ws_upgrades (reactor_handle)
					end
					touch (a_now_ms)
				else
					append_error_response (400, "Bad Request")
					mark_closing
				end
			else
				-- Invalid WebSocket upgrade (missing key, bad version, etc.)
				append_error_response (400, "Bad Request")
				mark_closing
			end
		end

	process_websocket_frames (a_now_ms: NATURAL_64)
			-- Incrementally parse and dispatch WebSocket frames in `in_buf`.
		local
			consumed: INTEGER
			done: BOOLEAN
		do
			from
				done := False
			until
				done or in_buf_bytes < 2 or state /= State_open
			loop
				consumed := ws_parser.parse_frame (in_buf, in_read_offset, in_buf_bytes)
				if consumed > 0 then
					if reactor_handle /= default_pointer then
						{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_in (reactor_handle)
					end
					in_read_offset := in_read_offset + consumed
					if in_read_offset = in_write_offset then
						in_read_offset := 0
						in_write_offset := 0
					end
					touch (a_now_ms)
					dispatch_ws_frame (a_now_ms)
				elseif consumed = 0 then
					done := True
				else
					-- Protocol error (consumed is -1002, -1009, etc.)
					send_ws_close_frame ((-consumed).to_natural_16)
					mark_closing
					done := True
				end
			end
		end

	dispatch_ws_frame (a_now_ms: NATURAL_64)
			-- Dispatch decoded frame according to its opcode.
		local
			op: INTEGER
		do
			op := ws_parser.last_opcode
			if op = Ws_opcode_ping then
				send_ws_pong_frame (in_buf.item + ws_parser.last_payload_offset, ws_parser.last_payload_length)
			elseif op = Ws_opcode_pong then
				-- Heartbeat response acknowledged (touch already refreshed timestamp)
			elseif op = Ws_opcode_close then
				send_ws_close_frame (ws_parser.close_status_code)
				mark_closing
			elseif op = Ws_opcode_text or op = Ws_opcode_binary or op = Ws_opcode_continuation then
				if ws_parser.is_fin and frag_bytes = 0 then
					-- Unfragmented message
					on_ws_message (in_buf.item + ws_parser.last_payload_offset, ws_parser.last_payload_length, op)
				else
					-- Fragmented message accumulation
					if frag_bytes = 0 then
						frag_opcode := op
					end
					append_frag_payload (in_buf.item + ws_parser.last_payload_offset, ws_parser.last_payload_length)
					if frag_bytes > Max_ws_message_size then
						send_ws_close_frame (Ws_close_message_too_big)
						mark_closing
					elseif ws_parser.is_fin then
						if attached frag_buf as fb then
							on_ws_message (fb.item, frag_bytes, frag_opcode)
						end
						frag_bytes := 0
					end
				end
			end
		end

	on_ws_message (a_data: POINTER; a_len, a_opcode: INTEGER)
			-- Handle complete application WebSocket frame (echo or broadcast).
		local
			hdr: MANAGED_POINTER
			hdr_len: INTEGER
			is_pub, is_pub_sse: BOOLEAN
		do
			is_pub := {VR_REACTOR_BRIDGE}.c_has_pub_prefix (a_data, a_len)
			is_pub_sse := {VR_REACTOR_BRIDGE}.c_has_pub_sse_prefix (a_data, a_len)
			if is_pub and attached broadcast_handler as bh then
				bh.call ([Room_default, a_opcode, a_data + 4, a_len - 4])
			elseif is_pub_sse and attached sse_broadcast_handler as sbh then
				sbh.call ([Room_default, a_data + 8, a_len - 8])
			else
				-- Echo frame back to client
				create hdr.make (10)
				hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | a_opcode).to_natural_8, a_len, hdr.item)
				append_outbound (hdr.item, hdr_len)
				if a_len > 0 then
					append_outbound (a_data, a_len)
				end
				if reactor_handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_out (reactor_handle)
				end
			end
		end

	send_ws_pong_frame (a_payload: POINTER; a_len: INTEGER)
			-- Transmit unmasked Pong frame echoing received Ping payload.
		local
			hdr: MANAGED_POINTER
			hdr_len: INTEGER
		do
			create hdr.make (10)
			hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | Ws_opcode_pong).to_natural_8, a_len, hdr.item)
			append_outbound (hdr.item, hdr_len)
			if a_len > 0 then
				append_outbound (a_payload, a_len)
			end
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_out (reactor_handle)
			end
		end

	send_ws_close_frame (a_code: NATURAL_16)
			-- Transmit unmasked Close frame with status code.
		local
			hdr: MANAGED_POINTER
			body: MANAGED_POINTER
			hdr_len: INTEGER
		do
			create hdr.make (10)
			create body.make (2)
			body.put_natural_16_be (a_code, 0)
			hdr_len := {VR_REACTOR_BRIDGE}.c_ws_encode_frame_header ((0x80 | Ws_opcode_close).to_natural_8, 2, hdr.item)
			append_outbound (hdr.item, hdr_len)
			append_outbound (body.item, 2)
			proto_state := Proto_ws_closing
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_out (reactor_handle)
			end
		end

	append_frag_payload (a_src: POINTER; a_len: INTEGER)
			-- Accumulate payload chunk into lazy fragmentation buffer.
		local
			fb: MANAGED_POINTER
			needed: INTEGER
		do
			if not attached frag_buf as f then
				create fb.make (Max_ws_message_size.min (a_len.max (16384)))
				frag_buf := fb
			else
				fb := f
			end
			if frag_bytes + a_len > fb.count then
				needed := ((frag_bytes + a_len) * 2).min (Max_ws_message_size + 4096)
				fb.resize (needed)
			end
			if frag_bytes + a_len <= fb.count then
				{VR_REACTOR_BRIDGE}.c_memmove (fb.item + frag_bytes, a_src, a_len)
			end
			frag_bytes := frag_bytes + a_len
		end

	handle_http_request (a_now_ms: NATURAL_64)
			-- Dispatch parsed HTTP request to fast path or standard response.
		do
			if http_parser.method = Method_options then
				append_options_response
			elseif http_parser.method = Method_get and then
			   http_parser.path_slice.same_as_string (in_buf, "/healthz") then
				append_static_healthz_response
			elseif http_parser.method = Method_get and then
			       http_parser.path_slice.same_as_string (in_buf, "/metrics") then
				append_metrics_response
			elseif (http_parser.method = Method_post or http_parser.method = Method_get) and then
			       http_parser.path_slice.same_as_string (in_buf, "/debug/fault") then
				handle_debug_fault
			elseif http_parser.method = Method_get and then
			       (http_parser.path_slice.same_as_string (in_buf, "/") or
			        http_parser.path_slice.same_as_string (in_buf, "/index.html") or
			        http_parser.path_slice.same_as_string (in_buf, "/dashboard")) then
				append_dashboard_html_response
			elseif http_parser.method = Method_get and then
			       http_parser.path_slice.same_as_string (in_buf, "/echo") then
				append_standard_response ("VoidRush HTTP/1.1 Engine OK")
			else
				append_error_response (404, "Not Found")
			end
		end

	append_options_response
			-- Immediate 200 OK response for CORS preflight.
		local
			resp: STRING_8
		do
			resp := "HTTP/1.1 200 OK%R%NAccess-Control-Allow-Origin: *%R%NAccess-Control-Allow-Methods: GET, POST, OPTIONS%R%NAccess-Control-Allow-Headers: Content-Type, Last-Event-ID, Accept%R%NContent-Length: 0%R%NConnection: keep-alive%R%N%R%N"
			append_outbound_string (resp)
		end

	append_dashboard_html_response
			-- Serve Web Dashboard HTML client.
		local
			f: PLAIN_TEXT_FILE
			resp, html: STRING_8
		do
			create f.make_with_name ("example/web/index.html")
			if not f.exists then
				create f.make_with_name ("/opt/eiffel/src/example/web/index.html")
			end
			if f.exists and then f.is_readable then
				f.open_read
				f.read_stream (f.count)
				html := f.last_string
				f.close
			else
				html := "<!DOCTYPE html><html><body><h1>VoidRush Telemetry Gateway</h1><p>Visit /events or /metrics.</p></body></html>"
			end
			create resp.make (256 + html.count)
			resp.append ("HTTP/1.1 200 OK%R%NContent-Type: text/html; charset=utf-8%R%NAccess-Control-Allow-Origin: *%R%NContent-Length: ")
			resp.append (html.count.out)
			if http_parser.is_keep_alive then
				resp.append ("%R%NConnection: keep-alive%R%N%R%N")
			else
				resp.append ("%R%NConnection: close%R%N%R%N")
			end
			resp.append (html)
			append_outbound_string (resp)
		end

	handle_debug_fault
			-- Execute controlled fault injection based on query parameters.
		local
			w_id: INTEGER
		do
			if http_parser.query_slice.has_substring_case_insensitive (in_buf, "worker") then
				if http_parser.query_slice.has_substring_case_insensitive (in_buf, "id=1") then
					w_id := 1
				elseif http_parser.query_slice.has_substring_case_insensitive (in_buf, "id=2") then
					w_id := 2
				elseif http_parser.query_slice.has_substring_case_insensitive (in_buf, "id=3") then
					w_id := 3
				elseif http_parser.query_slice.has_substring_case_insensitive (in_buf, "id=0") then
					w_id := 0
				else
					w_id := fd \\ 4
				end
				if reactor_handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_reactor_report_worker_fault (reactor_handle, w_id)
					{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
				end
				append_standard_response ("Fault Injected: Worker #" + w_id.out + " Crashed%N")
			elseif http_parser.query_slice.has_substring_case_insensitive (in_buf, "broker") then
				if reactor_handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
					{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
				end
				append_standard_response ("Fault Injected: Broker Crashed%N")
			else
				-- Connection-level fault simulation: triggers rescue on current_fd
				check deliberate_connection_fault: False end
			end
		end

	append_metrics_response
			-- Expose Prometheus metrics from native reactor.
		local
			m: VR_METRICS
			body: STRING_8
		do
			if reactor_handle /= default_pointer then
				create m.make_from_pointer (reactor_handle)
				body := m.to_prometheus_format
			else
				body := "# No active reactor%N"
			end
			append_standard_response (body)
		end

feature -- Response Builders (Zero/Low Allocation)

	append_static_healthz_response
			-- Immediate 200 OK fast-path response without allocating strings.
		local
			resp: STRING_8
		do
			if http_parser.is_keep_alive then
				resp := "HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NAccess-Control-Allow-Origin: *%R%NContent-Length: 2%R%NConnection: keep-alive%R%N%R%NOK"
			else
				resp := "HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NAccess-Control-Allow-Origin: *%R%NContent-Length: 2%R%NConnection: close%R%N%R%NOK"
			end
			append_outbound_string (resp)
		end

	append_standard_response (body: STRING_8)
			-- Generate 200 OK response containing `body`.
		require
			valid_body: body /= Void
		local
			resp: STRING_8
		do
			create resp.make (160 + body.count)
			resp.append ("HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NAccess-Control-Allow-Origin: *%R%NContent-Length: ")
			resp.append (body.count.out)
			if http_parser.is_keep_alive then
				resp.append ("%R%NConnection: keep-alive%R%N%R%N")
			else
				resp.append ("%R%NConnection: close%R%N%R%N")
			end
			resp.append (body)
			append_outbound_string (resp)
		end

	append_error_response (status_code: INTEGER; reason: STRING_8)
			-- Generate HTTP error response and force close.
		require
			valid_code: status_code >= 400 and status_code <= 599
			valid_reason: reason /= Void
		local
			resp: STRING_8
		do
			create resp.make (128)
			resp.append ("HTTP/1.1 ")
			resp.append (status_code.out)
			resp.append (" ")
			resp.append (reason)
			resp.append ("%R%NContent-Length: 0%R%NConnection: close%R%N%R%N")
			append_outbound_string (resp)
		end

feature -- Inbound & Outbound Buffer Compaction

	append_inbound (a_src: POINTER; a_len: INTEGER)
			-- Append `a_len` bytes from `a_src` to inbound buffer with in-place compaction.
		require
			valid_src: a_src /= default_pointer
			valid_len: a_len > 0
		local
			unread, needed: INTEGER
		do
			unread := in_write_offset - in_read_offset
			if in_write_offset + a_len > in_buf.count then
				if in_read_offset > 0 then
					if unread > 0 then
						{VR_REACTOR_BRIDGE}.c_memmove (in_buf.item, in_buf.item + in_read_offset, unread)
					end
					in_read_offset := 0
					in_write_offset := unread
				end
				if in_write_offset + a_len > in_buf.count then
					needed := ((in_write_offset + a_len) * 2).max (in_buf.count * 2)
					in_buf.resize (needed)
				end
			end
			{VR_REACTOR_BRIDGE}.c_memmove (in_buf.item + in_write_offset, a_src, a_len)
			in_write_offset := in_write_offset + a_len
		ensure
			bytes_added: in_buf_bytes = old in_buf_bytes + a_len
		end

	append_outbound (a_src: POINTER; a_len: INTEGER)
			-- Append `a_len` bytes from `a_src` to outbound buffer with in-place compaction.
		require
			valid_src: a_src /= default_pointer
			valid_len: a_len > 0
		local
			unread, needed: INTEGER
		do
			unread := out_write_offset - out_read_offset
			if out_write_offset + a_len > out_buf.count then
				if out_read_offset > 0 then
					if unread > 0 then
						{VR_REACTOR_BRIDGE}.c_memmove (out_buf.item, out_buf.item + out_read_offset, unread)
					end
					out_read_offset := 0
					out_write_offset := unread
				end
				if out_write_offset + a_len > out_buf.count then
					needed := ((out_write_offset + a_len) * 2).max (out_buf.count * 2)
					out_buf.resize (needed)
				end
			end
			{VR_REACTOR_BRIDGE}.c_memmove (out_buf.item + out_write_offset, a_src, a_len)
			out_write_offset := out_write_offset + a_len
		ensure
			bytes_appended: out_buf_bytes = old out_buf_bytes + a_len
		end

	append_echo (a_src: POINTER; a_len: INTEGER)
			-- Backward-compatible direct append to `out_buf` for TCP echo.
		require
			valid_src: a_src /= default_pointer
			valid_len: a_len > 0
		do
			append_outbound (a_src, a_len)
		ensure
			bytes_appended: out_buf_bytes = old out_buf_bytes + a_len
		end

	append_outbound_string (str: STRING_8)
			-- Append ASCII string directly into `out_buf` with compaction.
		require
			valid_str: str /= Void
		local
			unread, needed, len, i: INTEGER
		do
			len := str.count
			if len > 0 then
				unread := out_write_offset - out_read_offset
				if out_write_offset + len > out_buf.count then
					if out_read_offset > 0 then
						if unread > 0 then
							{VR_REACTOR_BRIDGE}.c_memmove (out_buf.item, out_buf.item + out_read_offset, unread)
						end
						out_read_offset := 0
						out_write_offset := unread
					end
					if out_write_offset + len > out_buf.count then
						needed := ((out_write_offset + len) * 2).max (out_buf.count * 2)
						out_buf.resize (needed)
					end
				end
				from
					i := 1
				invariant
					valid_i: i >= 1 and i <= len + 1
				until
					i > len
				loop
					out_buf.put_character (str.item (i), out_write_offset + i - 1)
					i := i + 1
				variant
					remaining: len + 1 - i
				end
				out_write_offset := out_write_offset + len
			end
		ensure
			bytes_added: out_buf_bytes = old out_buf_bytes + str.count
		end

	advance_read (a_n: INTEGER)
			-- Advance outbound read pointer after successful write.
		require
			valid_n: a_n > 0 and a_n <= out_buf_bytes
		do
			out_read_offset := out_read_offset + a_n
			if out_read_offset = out_write_offset then
				out_read_offset := 0
				out_write_offset := 0
			end
		ensure
			offset_valid: out_read_offset <= out_write_offset
		end

feature {NONE} -- Implementation

	in_buf: MANAGED_POINTER
			-- Inbound request buffer

	in_read_offset: INTEGER
			-- Inbound unparsed cursor

	in_write_offset: INTEGER
			-- Inbound free cursor

	out_buf: MANAGED_POINTER
			-- Outbound response buffer

	out_read_offset: INTEGER
			-- Outbound write cursor

	out_write_offset: INTEGER
			-- Outbound free cursor

invariant
	valid_fd: fd >= 0
	valid_generation: generation > 0
	valid_state: state = State_open or state = State_closing or state = State_closed
	valid_in_offsets: in_read_offset >= 0 and in_read_offset <= in_write_offset and in_write_offset <= in_buf.count
	valid_out_offsets: out_read_offset >= 0 and out_read_offset <= out_write_offset and out_write_offset <= out_buf.count

end
