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
			create scratch_val_slice.make_empty
			create scratch_name_slice.make_empty
			reset
		ensure
			reset_done: method = Method_unknown and header_bytes_consumed = 0
			watermark_cleared: scan_watermark = 0
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

	query_room_id (buf: MANAGED_POINTER): INTEGER
			-- Extract integer room ID from query string (e.g. ?room=42 or ?foo=1&room=42).
			-- Returns Room_default (0) if parameter is absent or non-numeric.
		require
			valid_buf: buf /= Void
		local
			pos, end_pos, digit_val: INTEGER
			found: BOOLEAN
			val: INTEGER_64
			c: CHARACTER
		do
			Result := Room_default
			if not query_slice.is_empty then
				pos := query_slice.offset
				end_pos := query_slice.offset + query_slice.length
				from
				until
					pos >= end_pos or found
				loop
					if pos + 5 <= end_pos and then
					   buf.read_character (pos) = 'r' and then
					   buf.read_character (pos + 1) = 'o' and then
					   buf.read_character (pos + 2) = 'o' and then
					   buf.read_character (pos + 3) = 'm' and then
					   buf.read_character (pos + 4) = '=' then
						val := 0
						from
							pos := pos + 5
						until
							pos >= end_pos or else buf.read_character (pos) = '&'
						loop
							c := buf.read_character (pos)
							if c >= '0' and c <= '9' then
								digit_val := c.code - ('0').code
								if val <= 214748364 then
									val := val * 10 + digit_val
								end
							else
								pos := end_pos
							end
							pos := pos + 1
						end
						if val >= 0 and val <= 2147483647 then
							Result := val.to_integer_32
							found := True
						end
					else
						from
						until
							pos >= end_pos or else buf.read_character (pos) = '&'
						loop
							pos := pos + 1
						end
						if pos < end_pos and then buf.read_character (pos) = '&' then
							pos := pos + 1
						end
					end
				end
			end
		ensure
			valid_room: Result >= 0
		end

	has_transfer_encoding: BOOLEAN
			-- Was a Transfer-Encoding header detected in the request?

	is_content_length_invalid: BOOLEAN
			-- Was Content-Length present but malformed, negative, or overflowing?

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
			scratch_name_slice.reset
			has_transfer_encoding := False
			is_content_length_invalid := False
			ws_version := 0
			version_minor := 1
			is_keep_alive := True
			content_length := 0
			header_bytes_consumed := 0
			scan_watermark := 0
		ensure
			method_cleared: method = Method_unknown
			consumed_cleared: header_bytes_consumed = 0
			watermark_cleared: scan_watermark = 0
			transfer_encoding_cleared: not has_transfer_encoding
			content_length_invalid_cleared: not is_content_length_invalid
		end

feature -- Parsing

	try_parse_request_header (buf: MANAGED_POINTER; start_offset, end_offset: INTEGER): BOOLEAN
			-- Attempt to scan and parse complete HTTP/1.1 headers between `start_offset` and `end_offset`.
			-- Returns True if complete headers ending in \r\n\r\n (or \n\n) were found and parsed.
			-- Bytes already scanned by a previous unsuccessful call for the same request are skipped
			-- (minus a 3-byte overlap for delimiters straddling the old boundary).
		require
			valid_buf: buf /= Void
			valid_range: start_offset >= 0 and end_offset <= buf.count and start_offset <= end_offset
		local
			header_end, req_line_end, scan_from: INTEGER
		do
			scan_from := (start_offset + scan_watermark - Delimiter_overlap).max (start_offset).min (end_offset)
			header_end := find_crlf_crlf (buf, scan_from, end_offset)
			if header_end > 0 then
				scan_watermark := 0
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
				scan_watermark := end_offset - start_offset
				Result := False
			end
		ensure
			watermark_cleared_on_success: Result implies scan_watermark = 0
			watermark_consistent: scan_watermark = 0 or scan_watermark = end_offset - start_offset
		end

	scan_watermark: INTEGER
			-- Number of bytes (relative to the request start) already scanned for the header terminator

feature {NONE} -- Delimiter Scanners

	Delimiter_overlap: INTEGER = 3
			-- Bytes re-scanned behind the watermark so a CRLFCRLF split across reads is detected

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
				method_slice.set (start_pos, 3)
				sp1 := start_pos + 3
			elseif line_end - start_pos >= 5 and then
			       buf.read_character (start_pos) = 'P' and then
			       buf.read_character (start_pos + 1) = 'O' and then
			       buf.read_character (start_pos + 2) = 'S' and then
			       buf.read_character (start_pos + 3) = 'T' and then
			       buf.read_character (start_pos + 4) = ' ' then
				method := Method_post
				method_slice.set (start_pos, 4)
				sp1 := start_pos + 4
			elseif line_end - start_pos >= 5 and then
			       buf.read_character (start_pos) = 'H' and then
			       buf.read_character (start_pos + 1) = 'E' and then
			       buf.read_character (start_pos + 2) = 'A' and then
			       buf.read_character (start_pos + 3) = 'D' and then
			       buf.read_character (start_pos + 4) = ' ' then
				method := Method_head
				method_slice.set (start_pos, 4)
				sp1 := start_pos + 4
			else
				sp1 := find_char (buf, ' ', start_pos, line_end)
				if sp1 > start_pos then
					method_slice.set (start_pos, sp1 - start_pos)
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
						path_slice.set (sp1 + 1, qmark - (sp1 + 1))
						query_slice.set (qmark + 1, sp2 - (qmark + 1))
					else
						path_slice.set (sp1 + 1, uri_len)
						query_slice.reset
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
			-- Classify header at `name_offset` and record its value window.
			-- Retained values are copied by offset/length (never by reference) into their dedicated slices.
		local
			name_len, parsed_len: INTEGER
			v_slice, n_slice: VR_HTTP_SLICE
		do
			name_len := colon_offset - name_offset
			n_slice := scratch_name_slice
			n_slice.set (name_offset, name_len)
			v_slice := scratch_val_slice
			v_slice.set (val_offset, val_len)

			if n_slice.same_as_string_case_insensitive (buf, "content-length") then
				parsed_len := v_slice.to_integer (buf)
				if parsed_len < 0 then
					is_content_length_invalid := True
				else
					content_length := parsed_len
				end
			elseif n_slice.same_as_string_case_insensitive (buf, "transfer-encoding") then
				has_transfer_encoding := True
			elseif n_slice.same_as_string_case_insensitive (buf, "connection") then
				if v_slice.same_as_string_case_insensitive (buf, "close") then
					is_keep_alive := False
				elseif v_slice.same_as_string_case_insensitive (buf, "keep-alive") then
					is_keep_alive := True
				end
				if v_slice.has_substring_case_insensitive (buf, "upgrade") then
					is_connection_upgrade := True
				end
			elseif n_slice.same_as_string_case_insensitive (buf, "upgrade") then
				if v_slice.same_as_string_case_insensitive (buf, "websocket") then
					is_upgrade_websocket := True
				end
			elseif n_slice.same_as_string_case_insensitive (buf, "sec-websocket-key") then
				ws_key_slice.set (val_offset, val_len)
			elseif n_slice.same_as_string_case_insensitive (buf, "sec-websocket-version") then
				ws_version := v_slice.to_integer (buf)
			elseif n_slice.same_as_string_case_insensitive (buf, "host") then
				host_slice.set (val_offset, val_len)
			elseif n_slice.same_as_string_case_insensitive (buf, "x-forwarded-for") then
				x_forwarded_for_slice.set (val_offset, val_len)
			elseif n_slice.same_as_string_case_insensitive (buf, "x-forwarded-proto") then
				x_forwarded_proto_slice.set (val_offset, val_len)
			elseif n_slice.same_as_string_case_insensitive (buf, "accept") then
				accept_slice.set (val_offset, val_len)
			elseif n_slice.same_as_string_case_insensitive (buf, "last-event-id") then
				last_event_id_slice.set (val_offset, val_len)
			end
		end

	scratch_val_slice: VR_HTTP_SLICE
			-- Reusable header-value window used for transient comparisons (never retained)

	scratch_name_slice: VR_HTTP_SLICE
			-- Reusable header-name window used for transient comparisons (never retained)

invariant
	valid_method: method >= Method_unknown and method <= Method_options
	valid_consumed: header_bytes_consumed >= 0
	valid_content_length: content_length >= 0
	valid_watermark: scan_watermark >= 0

end
