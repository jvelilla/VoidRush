note
	description: "Non-separate TCP connection state with dual in/out buffers and incremental HTTP/1.1 pipeline"

class
	VR_CONNECTION

inherit
	VR_REACTOR_CONSTANTS

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
			client_ip := "127.0.0.1"
			client_port := 0
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
					else
						-- Headers not yet complete
						if in_buf_bytes >= Max_header_size then
							append_error_response (431, "Request Header Fields Too Large")
							mark_closing
						end
						done := True
					end
				else
					done := True
				end
			end
		end

	handle_http_request (a_now_ms: NATURAL_64)
			-- Dispatch parsed HTTP request to fast path or standard response.
		do
			if http_parser.method = Method_get and then
			   http_parser.path_slice.same_as_string (in_buf, "/healthz") then
				append_static_healthz_response
			elseif http_parser.method = Method_get and then
			       (http_parser.path_slice.same_as_string (in_buf, "/") or
			        http_parser.path_slice.same_as_string (in_buf, "/echo")) then
				append_standard_response ("VoidRush HTTP/1.1 Engine OK")
			else
				append_error_response (404, "Not Found")
			end
		end

feature -- Response Builders (Zero/Low Allocation)

	append_static_healthz_response
			-- Immediate 200 OK fast-path response without allocating strings.
		local
			resp: STRING_8
		do
			if http_parser.is_keep_alive then
				resp := "HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NContent-Length: 2%R%NConnection: keep-alive%R%N%R%NOK"
			else
				resp := "HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NContent-Length: 2%R%NConnection: close%R%N%R%NOK"
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
			create resp.make (128 + body.count)
			resp.append ("HTTP/1.1 200 OK%R%NContent-Type: text/plain%R%NContent-Length: ")
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

	append_echo (a_src: POINTER; a_len: INTEGER)
			-- Backward-compatible direct append to `out_buf` for TCP echo.
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
				variant
					remaining: len + 1 - i
				until
					i > len
				loop
					out_buf.put_character (str.item (i), out_write_offset + i - 1)
					i := i + 1
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
