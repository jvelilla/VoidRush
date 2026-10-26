note
	description: "Non-separate zero-copy byte slice pointing into a parent buffer"

class
	VR_HTTP_SLICE

create
	make,
	make_empty

feature -- Initialization

	make (a_offset, a_length: INTEGER)
			-- Initialize slice starting at `a_offset` with `a_length` bytes.
		require
			valid_offset: a_offset >= 0
			valid_length: a_length >= 0
		do
			offset := a_offset
			length := a_length
		ensure
			offset_set: offset = a_offset
			length_set: length = a_length
		end

	make_empty
			-- Initialize empty slice.
		do
			offset := 0
			length := 0
		ensure
			is_empty: is_empty
		end

	reset
			-- Clear slice to empty.
		do
			offset := 0
			length := 0
		ensure
			is_empty: is_empty
		end

feature -- Element Change

	set (a_offset, a_length: INTEGER)
			-- Mutate slice window in-place without reallocation.
		require
			valid_offset: a_offset >= 0
			valid_length: a_length >= 0
		do
			offset := a_offset
			length := a_length
		ensure
			offset_set: offset = a_offset
			length_set: length = a_length
		end

feature -- Access

	offset: INTEGER
			-- Starting byte offset within parent buffer

	length: INTEGER
			-- Number of bytes referenced by this slice

feature -- Status Report

	is_empty: BOOLEAN
			-- Is this slice empty?
		do
			Result := length = 0
		end

feature -- Comparison & Conversion

	same_as_string (buf: MANAGED_POINTER; str: STRING_8): BOOLEAN
			-- Does byte slice in `buf` match ASCII `str` character by character?
		require
			valid_buf: buf /= Void
			valid_str: str /= Void
		local
			i: INTEGER
			matches: BOOLEAN
		do
			if length /= str.count then
				Result := False
			else
				matches := True
				from
					i := 1
				invariant
					valid_i: i >= 1 and i <= length + 1
				until
					not matches or i > length
				loop
					if buf.read_character (offset + i - 1) /= str.item (i) then
						matches := False
					end
					i := i + 1
				variant
					remaining: length + 1 - i
				end
				Result := matches
			end
		end

	same_as_string_case_insensitive (buf: MANAGED_POINTER; str: STRING_8): BOOLEAN
			-- Does byte slice in `buf` match ASCII `str` ignoring case?
		require
			valid_buf: buf /= Void
			valid_str: str /= Void
		local
			i: INTEGER
			matches: BOOLEAN
			c1, c2: CHARACTER
		do
			if length /= str.count then
				Result := False
			else
				matches := True
				from
					i := 1
				invariant
					valid_i: i >= 1 and i <= length + 1
				until
					not matches or i > length
				loop
					c1 := buf.read_character (offset + i - 1).as_lower
					c2 := str.item (i).as_lower
					if c1 /= c2 then
						matches := False
					end
					i := i + 1
				variant
					remaining: length + 1 - i
				end
				Result := matches
			end
		end

	has_substring_case_insensitive (buf: MANAGED_POINTER; token: STRING_8): BOOLEAN
			-- Does byte slice in `buf` contain ASCII substring `token` ignoring case?
		require
			valid_buf: buf /= Void
			valid_token: token /= Void
		local
			i, j: INTEGER
			matched: BOOLEAN
		do
			if length < token.count or token.count = 0 then
				Result := False
			else
				from
					i := 0
				invariant
					valid_i: i >= 0 and i <= length + 1
				until
					Result or (i + token.count > length)
				loop
					matched := True
					from
						j := 1
					invariant
						valid_j: j >= 1 and j <= token.count + 1
					until
						not matched or (j > token.count)
					loop
						if buf.read_character (offset + i + j - 1).as_lower /= token.item (j).as_lower then
							matched := False
						end
						j := j + 1
					variant
						rem_j: token.count + 1 - j
					end
					if matched then
						Result := True
					end
					i := i + 1
				variant
					remaining_i: length - token.count + 1 - i
				end
			end
		end

	starts_with (buf: MANAGED_POINTER; prefix: STRING_8): BOOLEAN
			-- Does byte slice in `buf` begin with `prefix`?
		require
			valid_buf: buf /= Void
			valid_prefix: prefix /= Void
		local
			i: INTEGER
			matches: BOOLEAN
		do
			if length < prefix.count then
				Result := False
			else
				matches := True
				from
					i := 1
				invariant
					valid_i: i >= 1 and i <= prefix.count + 1
				until
					not matches or i > prefix.count
				loop
					if buf.read_character (offset + i - 1) /= prefix.item (i) then
						matches := False
					end
					i := i + 1
				variant
					remaining: prefix.count + 1 - i
				end
				Result := matches
			end
		end

	to_integer (buf: MANAGED_POINTER): INTEGER
			-- Parse ASCII decimal integer represented by this slice.
			-- Returns -1 if invalid, empty, or on arithmetic overflow.
		require
			valid_buf: buf /= Void
		local
			i, val, d: INTEGER
			c: CHARACTER
			valid: BOOLEAN
		do
			if is_empty then
				Result := -1
			else
				val := 0
				valid := True
				from
					i := 0
				invariant
					valid_i: i >= 0 and i <= length
				until
					not valid or i >= length
				loop
					c := buf.read_character (offset + i)
					if c >= '0' and c <= '9' then
						d := c.code - ('0').code
						if val > ({INTEGER_32}.max_value - d) // 10 then
							valid := False
						else
							val := (val * 10) + d
						end
					else
						valid := False
					end
					i := i + 1
				variant
					remaining: length - i
				end
				if valid then
					Result := val
				else
					Result := -1
				end
			end
		end

	to_string_8 (buf: MANAGED_POINTER): STRING_8
			-- Materialize slice as STRING_8 (for diagnostics or non-hot paths).
		require
			valid_buf: buf /= Void
		local
			i: INTEGER
		do
			create Result.make (length)
			from
				i := 0
			invariant
				valid_i: i >= 0 and i <= length
			until
				i >= length
			loop
				Result.append_character (buf.read_character (offset + i))
				i := i + 1
			variant
				remaining: length - i
			end
		ensure
			length_matched: Result.count = length
		end

invariant
	valid_offset: offset >= 0
	valid_length: length >= 0

end
