note
	description: "Symbolic constants for reactor flags, non-blocking I/O codes, and buffer dimensions"

class
	VR_REACTOR_CONSTANTS

feature -- Event Flags

	Reactor_in: NATURAL_32 = 0x01
			-- Ready for reading

	Reactor_out: NATURAL_32 = 0x02
			-- Ready for writing

	Reactor_err: NATURAL_32 = 0x04
			-- Error condition

	Reactor_hup: NATURAL_32 = 0x08
			-- Hang up / peer closed

feature -- I/O Status Codes

	Reactor_io_ok: INTEGER = 1
			-- Operation completed successfully

	Reactor_io_eof: INTEGER = 0
			-- Peer closed connection (EOF)

	Reactor_io_would_block: INTEGER = -1
			-- Socket would block (EAGAIN / EWOULDBLOCK)

	Reactor_io_error: INTEGER = -2
			-- Fatal socket error

feature -- Buffer Configuration

	Event_size: INTEGER = 12
			-- Size in bytes of one native vr_reactor_event_t struct (fd: 4 + gen: 4 + flags: 4)

	Max_events: INTEGER = 256
			-- Maximum number of events returned in one vr_reactor_wait call

end
