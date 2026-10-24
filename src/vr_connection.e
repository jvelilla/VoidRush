note
	description: "Non-separate TCP connection state and outbound buffer owned strictly by a single VR_WORKER"

class
	VR_CONNECTION

inherit
	VR_REACTOR_CONSTANTS

create
	make,
	make_with_time

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
			buffer_empty: out_buf_bytes = 0
		end

	make_with_time (a_fd: INTEGER; a_generation: NATURAL_32; a_now_ms: NATURAL_64)
			-- Initialize connection for socket `a_fd` protected by `a_generation` at timestamp `a_now_ms`.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		do
			fd := a_fd
			generation := a_generation
			state := State_open
			last_active_ms := a_now_ms
			paused_in := False
			create buffer.make (Scratch_buffer_size)
			read_offset := 0
			write_offset := 0
		ensure
			fd_set: fd = a_fd
			generation_set: generation = a_generation
			is_open: is_open
			not_paused: not paused_in
			buffer_empty: out_buf_bytes = 0
		end

feature -- Access

	fd: INTEGER
			-- Socket file descriptor

	generation: NATURAL_32
			-- Monotonic generation token assigned at accept

	state: INTEGER
			-- Lifecycle state (State_open, State_closing, State_closed)

	last_active_ms: NATURAL_64
			-- Last activity timestamp in monotonic milliseconds

	paused_in: BOOLEAN
			-- True when flow control has paused Reactor_in interest

	out_buf_bytes: INTEGER
			-- Number of unwritten bytes in outbound buffer
		do
			Result := write_offset - read_offset
		ensure
			non_negative: Result >= 0
		end

	read_pointer: POINTER
			-- Direct memory pointer to unwritten bytes
		require
			has_bytes: out_buf_bytes > 0
		do
			Result := buffer.item + read_offset
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

feature -- Buffer Management

	append_echo (a_src: POINTER; a_len: INTEGER)
			-- Append `a_len` bytes from `a_src` to outbound buffer with in-place compaction.
		require
			valid_src: a_src /= default_pointer
			valid_len: a_len > 0
		local
			unread: INTEGER
			needed: INTEGER
		do
			unread := write_offset - read_offset
			if write_offset + a_len > buffer.count then
				-- Compaction: shift unread bytes back to offset 0 if space is wasted at head
				if read_offset > 0 then
					if unread > 0 then
						{VR_REACTOR_BRIDGE}.c_memmove (buffer.item, buffer.item + read_offset, unread)
					end
					read_offset := 0
					write_offset := unread
				end
				-- Resize only if compaction did not free enough capacity
				if write_offset + a_len > buffer.count then
					needed := ((write_offset + a_len) * 2).max (buffer.count * 2)
					buffer.resize (needed)
				end
			end
			{VR_REACTOR_BRIDGE}.c_memmove (buffer.item + write_offset, a_src, a_len)
			write_offset := write_offset + a_len
		ensure
			bytes_appended: out_buf_bytes = old out_buf_bytes + a_len
		end

	advance_read (a_n: INTEGER)
			-- Advance read pointer after successful write.
		require
			valid_n: a_n > 0 and a_n <= out_buf_bytes
		do
			read_offset := read_offset + a_n
			if read_offset = write_offset then
				read_offset := 0
				write_offset := 0
			end
		ensure
			offset_valid: read_offset <= write_offset
		end

feature {NONE} -- Implementation

	buffer: MANAGED_POINTER
			-- Outbound echo buffer

	read_offset: INTEGER
			-- Offset of next byte to write to socket

	write_offset: INTEGER
			-- Offset of next free byte for incoming echo

invariant
	valid_fd: fd >= 0
	valid_generation: generation > 0
	valid_state: state = State_open or state = State_closing or state = State_closed
	valid_offsets: read_offset >= 0 and read_offset <= write_offset and write_offset <= buffer.count

end
