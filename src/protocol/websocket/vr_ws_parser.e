note
	description: "Incremental RFC 6455 WebSocket frame parser operating directly over connection in_buf"

class
	VR_WS_PARSER

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Initialize parser with default state.
		do
			last_opcode := 0
			last_payload_offset := 0
			last_payload_length := 0
			is_fin := False
			is_control := False
			close_status_code := Ws_close_normal
		ensure
			no_opcode: last_opcode = 0
			not_control: not is_control
		end

feature -- Access

	last_opcode: INTEGER
			-- Opcode of the most recently parsed frame (0x1 text, 0x2 binary, 0x8 close, 0x9 ping, 0xA pong, 0x0 continuation)

	last_payload_offset: INTEGER
			-- Starting byte offset of unmasked payload within parent buffer

	last_payload_length: INTEGER
			-- Byte count of unmasked payload

	is_fin: BOOLEAN
			-- Was the FIN bit set on the most recently parsed frame?

	is_control: BOOLEAN
			-- Is the parsed frame a control frame (opcode >= 0x8)?

	close_status_code: NATURAL_16
			-- Extracted WebSocket status code if frame was Close (0x8), or 1000 default

feature -- Parsing Execution

	parse_frame (a_buf: MANAGED_POINTER; a_offset, a_available: INTEGER): INTEGER
			-- Attempt to parse a complete WebSocket frame from `a_buf` starting at `a_offset`.
			-- Returns bytes consumed if a complete frame was parsed, 0 if more data is needed,
			-- or a negative integer error code (e.g. -1002, -1009) on protocol violation.
		require
			valid_buf: a_buf /= Void and then a_buf.count > 0
			valid_offset: a_offset >= 0 and a_offset < a_buf.count
			valid_available: a_available >= 0 and a_available <= (a_buf.count - a_offset)
		local
			b0, b1, rsv, len7: NATURAL_8
			hdr_size, mask_offset, payload_len: INTEGER
			is_masked: BOOLEAN
		do
			if a_available < 2 then
				Result := 0 -- Need more bytes for basic 2-byte header
			else
				b0 := a_buf.read_natural_8 (a_offset)
				b1 := a_buf.read_natural_8 (a_offset + 1)

				is_fin := (b0 & 0x80) /= 0
				rsv := b0 & 0x70
				last_opcode := (b0 & 0x0F).to_integer_32
				is_masked := (b1 & 0x80) /= 0
				len7 := b1 & 0x7F
				is_control := (last_opcode >= Ws_opcode_close)

				if rsv /= 0 then
					Result := -1002 -- Protocol Error: RSV bits must be 0 without extensions
				elseif not is_masked then
					Result := -1002 -- Protocol Error: Client-to-server frames must be masked
				elseif is_control and then not is_fin then
					Result := -1002 -- Protocol Error: Control frames cannot be fragmented
				elseif is_control and then len7 > 125 then
					Result := -1002 -- Protocol Error: Control frame payload cannot exceed 125 bytes
				elseif last_opcode /= Ws_opcode_continuation and
				       last_opcode /= Ws_opcode_text and
				       last_opcode /= Ws_opcode_binary and
				       last_opcode /= Ws_opcode_close and
				       last_opcode /= Ws_opcode_ping and
				       last_opcode /= Ws_opcode_pong then
					Result := -1002 -- Protocol Error: Reserved/unknown opcode
				else
					-- Calculate header size and payload length
					if len7 <= 125 then
						hdr_size := 6 -- 2 bytes header + 4 bytes mask
						payload_len := len7.to_integer_32
					elseif len7 = 126 then
						hdr_size := 8 -- 2 + 2 bytes len + 4 bytes mask
						if a_available >= 4 then
							payload_len := a_buf.read_natural_16_be (a_offset + 2).to_integer_32
						else
							payload_len := -1 -- Need more bytes
						end
					else -- len7 = 127
						hdr_size := 14 -- 2 + 8 bytes len + 4 bytes mask
						if a_available >= 10 then
							payload_len := a_buf.read_natural_64_be (a_offset + 2).to_integer_32
						else
							payload_len := -1 -- Need more bytes
						end
					end

					if payload_len < 0 or else a_available < hdr_size then
						Result := 0 -- Wait for complete extended length header
					elseif payload_len > Max_ws_message_size then
						Result := -1009 -- Message Too Big
					elseif a_available < (hdr_size + payload_len) then
						Result := 0 -- Wait for complete frame payload bytes
					else
						-- Complete frame present! Unmask in-place in C
						mask_offset := a_offset + hdr_size - 4
						last_payload_offset := a_offset + hdr_size
						last_payload_length := payload_len

						if payload_len > 0 then
							{VR_REACTOR_BRIDGE}.c_ws_unmask (
								a_buf.item + last_payload_offset,
								payload_len,
								a_buf.item + mask_offset
							)
						end

						-- Extract status code if Close frame
						if last_opcode = Ws_opcode_close then
							if payload_len >= 2 then
								close_status_code := a_buf.read_natural_16_be (last_payload_offset)
							else
								close_status_code := Ws_close_normal
							end
						end

						Result := hdr_size + payload_len
					end
				end
			end
		ensure
			valid_result: Result >= -1011 and Result <= a_available
		end

invariant
	valid_opcode: last_opcode >= 0 and last_opcode <= 15
	valid_payload_offset: last_payload_offset >= 0
	valid_payload_length: last_payload_length >= 0

end
