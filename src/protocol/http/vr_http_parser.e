note
	description: "Non-separate zero-allocation incremental HTTP/1.1 request header parser"

class
	VR_HTTP_PARSER

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Initialize parser with empty state.
		do
			create method_slice.make_empty
			create path_slice.make_empty
			create query_slice.make_empty
			create host_slice.make_empty
			create x_forwarded_for_slice.make_empty
			create x_forwarded_proto_slice.make_empty
			create ws_key_slice.make_empty
			create accept_slice.make_empty
			create last_event_id_slice.make_empty
			reset
		ensure
			reset_done: method = Method_unknown and header_bytes_consumed = 0
		end

feature -- Access

	method: INTEGER
			-- Parsed HTTP method code (Method_get, Method_post, etc.)

	method_slice: VR_HTTP_SLICE
			-- Exact slice containing HTTP method ASCII token

	path_slice: VR_HTTP_SLICE
			-- Exact slice containing request path (excluding query string)

	query_slice: VR_HTTP_SLICE
			-- Slice containing query string (excluding leading '?'), or empty

	version_minor: INTEGER
			-- 0 for HTTP/1.0, 1 for HTTP/1.1

	is_keep_alive: BOOLEAN
			-- Should connection remain open after response?

	content_length: INTEGER
			-- Declared payload length, or 0 if absent

	header_bytes_consumed: INTEGER
			-- Total bytes consumed by request line and headers (including CRLFCRLF)

	host_slice: VR_HTTP_SLICE
			-- Value slice of Host header, or empty if absent

	x_forwarded_for_slice: VR_HTTP_SLICE
			-- Value slice of X-Forwarded-For header, or empty if absent

	x_forwarded_proto_slice: VR_HTTP_SLICE
			-- Value slice of X-Forwarded-Proto header, or empty if absent

	is_upgrade_websocket: BOOLEAN
			-- Has "Upgrade: websocket" header been parsed?

	is_connection_upgrade: BOOLEAN
			-- Does "Connection" header contain "upgrade" token?

	ws_key_slice: VR_HTTP_SLICE
			-- Value slice of Sec-WebSocket-Key header (must be 24 characters)

	ws_version: INTEGER
			-- Parsed Sec-WebSocket-Version (must be 13)

	accept_slice: VR_HTTP_SLICE
			-- Value slice of Accept header, or empty if absent

	last_event_id_slice: VR_HTTP_SLICE
			-- Value slice of Last-Event-ID header, or empty if absent

	is_websocket_upgrade_request: BOOLEAN
			-- Is this request a valid RFC 6455 WebSocket upgrade request?
		do
			Result := method = Method_get and
			          is_upgrade_websocket and
			          is_connection_upgrade and
			          ws_key_slice.length = 24 and
			          ws_version = 13
		end

	is_sse_request (buf: MANAGED_POINTER): BOOLEAN
			-- Does request match GET /events or Accept: text/event-stream?
		require
			valid_buf: buf /= Void
		do
			Result := method = Method_get and then
			          (path_slice.same_as_string (buf, "/events") or else
			           accept_slice.has_substring_case_insensitive (buf, "text/event-stream"))
		end

	is_sse_publish_request (buf: MANAGED_POINTER): BOOLEAN
			-- Does request match POST /events (or POST /publish)?
		require
			valid_buf: buf /= Void
		do
			Result := method = Method_post and then
			          (path_slice.same_as_string (buf, "/events") or else
			           path_slice.same_as_string (buf, "/publish"))
		end

feature -- State Management

	reset
			-- Reset all parser token slices for the next request.
		do
			method := Method_unknown
			method_slice.reset
			path_slice.reset
			query_slice.reset
			host_slice.reset
			x_forwarded_for_slice.reset
			x_forwarded_proto_slice.reset
			is_upgrade_websocket := False
			is_connection_upgrade := False
			ws_key_slice.reset
			accept_slice.reset
			last_event_id_slice.reset
			ws_version := 0
			version_minor := 1
			is_keep_alive := True
			content_length := 0
			header_bytes_consumed := 0
		ensure
			method_cleared: method = Method_unknown
			consumed_cleared: header_bytes_consumed = 0
		end

feature -- Parsing

	try_parse_request_header (buf: MANAGED_POINTER; start_offset, end_offset: INTEGER): BOOLEAN
			-- Attempt to scan and parse complete HTTP/1.1 headers between `start_offset` and `end_offset`.
			-- Returns True if complete headers ending in \r\n\r\n (or \n\n) were found and parsed.
		require
			valid_buf: buf /= Void
			valid_range: start_offset >= 0 and end_offset <= buf.count and start_offset <= end_offset
		local
			header_end, req_line_end: INTEGER
		do
			header_end := find_crlf_crlf (buf, start_offset, end_offset)
			if header_end > 0 then
				header_bytes_consumed := header_end - start_offset
				req_line_end := find_line_end (buf, start_offset, header_end)
				if req_line_end > start_offset then
					parse_request_line (buf, start_offset, req_line_end)
					parse_headers (buf, req_line_end, header_end)
					Result := True
				else
					Result := False
				end
			else
				Result := False
			end
		end

feature {NONE} -- Delimiter Scanners

	find_crlf_crlf (buf: MANAGED_POINTER; start_pos, end_pos: INTEGER): INTEGER
			-- Offset immediately following \r\n\r\n or \n\n, or 0 if not found
		local
			i: INTEGER
		do
			Result := 0
			from
				i := start_pos
			invariant
				valid_i: i >= start_pos and i <= end_pos + 1
			until
				Result > 0 or i > end_pos - 2
			loop
				if buf.read_character (i) = '%R' and then
				   i + 3 < end_pos and then
				   buf.read_character (i + 1) = '%N' and then
				   buf.read_character (i + 2) = '%R' and then
				   buf.read_character (i + 3) = '%N' then
					Result := i + 4
				elseif buf.read_character (i) = '%N' and then
				       i + 1 < end_pos and then
				       buf.read_character (i + 1) = '%N' then
					Result := i + 2
				end
				i := i + 1
			variant
				remaining: end_pos + 1 - i
			end
		end

	find_line_end (buf: MANAGED_POINTER; start_pos, end_pos: INTEGER): INTEGER
			-- Offset immediately following the first \r\n or \n, or end_pos
		local
			i: INTEGER
		do
			Result := 0
			from
				i := start_pos
			invariant
				valid_i: i >= start_pos and i <= end_pos + 1
			until
				Result > 0 or i >= end_pos
			loop
				if buf.read_character (i) = '%N' then
					Result := i + 1
				end
				i := i + 1
			variant
				remaining: end_pos + 1 - i
			end
			if Result = 0 then
				Result := end_pos
			end
		end

	find_char (buf: MANAGED_POINTER; c: CHARACTER; start_pos, end_pos: INTEGER): INTEGER
			-- Offset of character `c`, or 0 if not found
		local
			i: INTEGER
		do
			Result := 0
			from
				i := start_pos
			invariant
				valid_i: i >= start_pos and i <= end_pos + 1
			until
				Result > 0 or i >= end_pos
			loop
				if buf.read_character (i) = c then
					Result := i
				end
				i := i + 1
			variant
				remaining: end_pos + 1 - i
			end
		end

feature {NONE} -- Token Extraction

	parse_request_line (buf: MANAGED_POINTER; start_pos, line_end: INTEGER)
		local
			sp1, sp2, qmark, uri_len: INTEGER
		do
			-- Identify method
			if line_end - start_pos >= 4 and then
			   buf.read_character (start_pos) = 'G' and then
			   buf.read_character (start_pos + 1) = 'E' and then
			   buf.read_character (start_pos + 2) = 'T' and then
			   buf.read_character (start_pos + 3) = ' ' then
				method := Method_get
				create method_slice.make (start_pos, 3)
				sp1 := start_pos + 3
			elseif line_end - start_pos >= 5 and then
			       buf.read_character (start_pos) = 'P' and then
			       buf.read_character (start_pos + 1) = 'O' and then
			       buf.read_character (start_pos + 2) = 'S' and then
			       buf.read_character (start_pos + 3) = 'T' and then
			       buf.read_character (start_pos + 4) = ' ' then
				method := Method_post
				create method_slice.make (start_pos, 4)
				sp1 := start_pos + 4
			elseif line_end - start_pos >= 5 and then
			       buf.read_character (start_pos) = 'H' and then
			       buf.read_character (start_pos + 1) = 'E' and then
			       buf.read_character (start_pos + 2) = 'A' and then
			       buf.read_character (start_pos + 3) = 'D' and then
			       buf.read_character (start_pos + 4) = ' ' then
				method := Method_head
				create method_slice.make (start_pos, 4)
				sp1 := start_pos + 4
			else
				sp1 := find_char (buf, ' ', start_pos, line_end)
				if sp1 > start_pos then
					create method_slice.make (start_pos, sp1 - start_pos)
					method := Method_unknown
				end
			end

			-- Parse URI path and query
			if sp1 > 0 then
				sp2 := find_char (buf, ' ', sp1 + 1, line_end)
				if sp2 > sp1 then
					uri_len := sp2 - sp1 - 1
					qmark := find_char (buf, '?', sp1 + 1, sp2)
					if qmark > 0 then
						create path_slice.make (sp1 + 1, qmark - (sp1 + 1))
						create query_slice.make (qmark + 1, sp2 - (qmark + 1))
					else
						create path_slice.make (sp1 + 1, uri_len)
						create query_slice.make_empty
					end

					-- Parse HTTP version: inspect character at sp2 + 8 (e.g. "HTTP/1.1")
					if sp2 + 8 <= line_end and then
					   buf.read_character (sp2 + 6) = '1' and then
					   buf.read_character (sp2 + 7) = '.' and then
					   buf.read_character (sp2 + 8) = '0' then
						version_minor := 0
						is_keep_alive := False
					else
						version_minor := 1
						is_keep_alive := True
					end
				end
			end
		end

	parse_headers (buf: MANAGED_POINTER; start_pos, header_end: INTEGER)
		local
			cur, line_next, colon, val_start, val_end: INTEGER
		do
			from
				cur := start_pos
			invariant
				valid_cur: cur >= start_pos and cur <= header_end + 1
			until
				cur >= header_end
			loop
				line_next := find_line_end (buf, cur, header_end)
				colon := find_char (buf, ':', cur, line_next)
				if colon > cur then
					-- Trim value leading whitespace
					val_start := colon + 1
					from
					invariant
						valid_v: val_start >= colon + 1 and val_start <= line_next + 1
					until
						val_start >= line_next or else
						(buf.read_character (val_start) /= ' ' and buf.read_character (val_start) /= '%T')
					loop
						val_start := val_start + 1
					variant
						rem: line_next + 1 - val_start
					end

					-- Trim value trailing CR/LF/spaces
					val_end := line_next
					from
					invariant
						valid_end: val_end >= val_start - 1 and val_end <= line_next
					until
						val_end <= val_start or else
						(buf.read_character (val_end - 1) /= '%R' and
						 buf.read_character (val_end - 1) /= '%N' and
						 buf.read_character (val_end - 1) /= ' ')
					loop
						val_end := val_end - 1
					variant
						rem: val_end - val_start + 2
					end

					if val_end >= val_start then
						match_header (buf, cur, colon, val_start, val_end - val_start)
					end
				end
				cur := line_next
			variant
				remaining: header_end + 1 - cur
			end
		end

	match_header (buf: MANAGED_POINTER; name_offset, colon_offset, val_offset, val_len: INTEGER)
		local
			name_len: INTEGER
			v_slice: VR_HTTP_SLICE
		do
			name_len := colon_offset - name_offset
			create v_slice.make (val_offset, val_len)

			-- Check "content-length" (14 chars)
			if name_len = 14 and then
			   (buf.read_character (name_offset) = 'C' or buf.read_character (name_offset) = 'c') and then
			   (buf.read_character (name_offset + 8) = 'L' or buf.read_character (name_offset + 8) = 'l') then
				content_length := v_slice.to_integer (buf).max (0)
			-- Check "connection" (10 chars)
			elseif name_len = 10 and then
			       (buf.read_character (name_offset) = 'C' or buf.read_character (name_offset) = 'c') and then
			       (buf.read_character (name_offset + 1) = 'O' or buf.read_character (name_offset + 1) = 'o') then
				if v_slice.same_as_string (buf, "close") or v_slice.same_as_string (buf, "Close") then
					is_keep_alive := False
				elseif v_slice.same_as_string (buf, "keep-alive") or v_slice.same_as_string (buf, "Keep-Alive") then
					is_keep_alive := True
				end
				if v_slice.has_substring_case_insensitive (buf, "upgrade") then
					is_connection_upgrade := True
				end
			-- Check "upgrade" (7 chars)
			elseif name_len = 7 and then
			       (buf.read_character (name_offset) = 'U' or buf.read_character (name_offset) = 'u') and then
			       (buf.read_character (name_offset + 1) = 'P' or buf.read_character (name_offset + 1) = 'p') then
				if v_slice.same_as_string_case_insensitive (buf, "websocket") then
					is_upgrade_websocket := True
				end
			-- Check "sec-websocket-key" (17 chars)
			elseif name_len = 17 and then
			       (buf.read_character (name_offset) = 'S' or buf.read_character (name_offset) = 's') and then
			       (buf.read_character (name_offset + 14) = 'K' or buf.read_character (name_offset + 14) = 'k') then
				ws_key_slice := v_slice
			-- Check "sec-websocket-version" (21 chars)
			elseif name_len = 21 and then
			       (buf.read_character (name_offset) = 'S' or buf.read_character (name_offset) = 's') and then
			       (buf.read_character (name_offset + 14) = 'V' or buf.read_character (name_offset + 14) = 'v') then
				ws_version := v_slice.to_integer (buf)
			-- Check "host" (4 chars)
			elseif name_len = 4 and then
			       (buf.read_character (name_offset) = 'H' or buf.read_character (name_offset) = 'h') then
				host_slice := v_slice
			-- Check "x-forwarded-for" (15 chars)
			elseif name_len = 15 and then
			       (buf.read_character (name_offset) = 'X' or buf.read_character (name_offset) = 'x') and then
			       (buf.read_character (name_offset + 12) = 'F' or buf.read_character (name_offset + 12) = 'f') then
				x_forwarded_for_slice := v_slice
			-- Check "x-forwarded-proto" (17 chars)
			elseif name_len = 17 and then
			       (buf.read_character (name_offset) = 'X' or buf.read_character (name_offset) = 'x') and then
			       (buf.read_character (name_offset + 12) = 'P' or buf.read_character (name_offset + 12) = 'p') then
				x_forwarded_proto_slice := v_slice
			-- Check "accept" (6 chars)
			elseif name_len = 6 and then
			       (buf.read_character (name_offset) = 'A' or buf.read_character (name_offset) = 'a') and then
			       (buf.read_character (name_offset + 1) = 'C' or buf.read_character (name_offset + 1) = 'c') then
				accept_slice := v_slice
			-- Check "last-event-id" (13 chars)
			elseif name_len = 13 and then
			       (buf.read_character (name_offset) = 'L' or buf.read_character (name_offset) = 'l') and then
			       (buf.read_character (name_offset + 5) = 'E' or buf.read_character (name_offset + 5) = 'e') then
				last_event_id_slice := v_slice
			end
		end

invariant
	valid_method: method >= Method_unknown and method <= Method_options
	valid_consumed: header_bytes_consumed >= 0
	valid_content_length: content_length >= 0

end
