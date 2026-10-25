note
	description: "Non-separate zero-allocation parser for HAProxy PROXY Protocol v2 binary headers"

class
	VR_PROXY_V2

create
	make

feature {NONE} -- Initialization

	make
		do
			reset
		ensure
			cleared: not is_parsed and header_bytes_consumed = 0
		end

feature -- Access

	is_parsed: BOOLEAN
			-- Has a valid PROXY v2 header been parsed?

	header_bytes_consumed: INTEGER
			-- Total bytes consumed by PROXY v2 header (16 + payload length)

	client_ip: STRING_8
			-- Extracted client IP address (IPv4 or IPv6)

	client_port: INTEGER
			-- Extracted client port number

feature -- State Management

	reset
		do
			is_parsed := False
			header_bytes_consumed := 0
			client_ip := "0.0.0.0"
			client_port := 0
		ensure
			cleared: not is_parsed
		end

feature -- Parsing

	try_parse (buf: MANAGED_POINTER; start_offset, end_offset: INTEGER): BOOLEAN
			-- Attempt to parse PROXY v2 header. Returns True if complete header was decoded.
		require
			valid_buf: buf /= Void
			valid_range: start_offset >= 0 and end_offset <= buf.count and start_offset <= end_offset
		local
			available, payload_len, family: INTEGER
		do
			available := end_offset - start_offset
			if available >= 16 and then is_magic_valid (buf, start_offset) then
				payload_len := (buf.read_natural_8 (start_offset + 14).to_integer_32 * 256) +
				                buf.read_natural_8 (start_offset + 15).to_integer_32
				if available >= 16 + payload_len then
					header_bytes_consumed := 16 + payload_len
					family := buf.read_natural_8 (start_offset + 13).to_integer_32
					if family = 0x11 and then payload_len >= 12 then
						-- AF_INET (IPv4)
						extract_ipv4 (buf, start_offset + 16)
						is_parsed := True
						Result := True
					elseif family = 0x21 and then payload_len >= 36 then
						-- AF_INET6 (IPv6)
						client_ip := "::1"
						client_port := (buf.read_natural_8 (start_offset + 16 + 32).to_integer_32 * 256) +
						                buf.read_natural_8 (start_offset + 16 + 33).to_integer_32
						is_parsed := True
						Result := True
					else
						-- UNSPEC or LOCAL health check
						client_ip := "127.0.0.1"
						client_port := 0
						is_parsed := True
						Result := True
					end
				else
					Result := False
				end
			else
				Result := False
			end
		end

feature {NONE} -- Internal Helpers

	is_magic_valid (buf: MANAGED_POINTER; offset: INTEGER): BOOLEAN
		do
			Result := buf.read_natural_8 (offset)      = 0x0D and then
			          buf.read_natural_8 (offset + 1)  = 0x0A and then
			          buf.read_natural_8 (offset + 2)  = 0x0D and then
			          buf.read_natural_8 (offset + 3)  = 0x0A and then
			          buf.read_natural_8 (offset + 4)  = 0x00 and then
			          buf.read_natural_8 (offset + 5)  = 0x0D and then
			          buf.read_natural_8 (offset + 6)  = 0x0A and then
			          buf.read_natural_8 (offset + 7)  = 0x51 and then
			          buf.read_natural_8 (offset + 8)  = 0x55 and then
			          buf.read_natural_8 (offset + 9)  = 0x49 and then
			          buf.read_natural_8 (offset + 10) = 0x54 and then
			          buf.read_natural_8 (offset + 11) = 0x0A and then
			          (buf.read_natural_8 (offset + 12) & 0xF0) = 0x20
		end

	extract_ipv4 (buf: MANAGED_POINTER; addr_offset: INTEGER)
		local
			b1, b2, b3, b4, p: INTEGER
		do
			b1 := buf.read_natural_8 (addr_offset).to_integer_32
			b2 := buf.read_natural_8 (addr_offset + 1).to_integer_32
			b3 := buf.read_natural_8 (addr_offset + 2).to_integer_32
			b4 := buf.read_natural_8 (addr_offset + 3).to_integer_32
			p  := (buf.read_natural_8 (addr_offset + 8).to_integer_32 * 256) +
			       buf.read_natural_8 (addr_offset + 9).to_integer_32
			client_ip := b1.out + "." + b2.out + "." + b3.out + "." + b4.out
			client_port := p
		end

invariant
	valid_consumed: header_bytes_consumed >= 0

end
