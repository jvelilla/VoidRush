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

feature -- Connection States

	State_open: INTEGER = 1
			-- Connection is active and processing events

	State_closing: INTEGER = 2
			-- Connection is in process of closing

	State_closed: INTEGER = 3
			-- Connection has been closed and removed

feature -- Fairness Budgets & Flow Control

	Read_budget_per_turn: INTEGER = 65536
			-- 64 KiB maximum read allowance per on_socket_event turn

	Write_budget_per_turn: INTEGER = 65536
			-- 64 KiB maximum write allowance per on_socket_event turn

	Out_buf_high_water: INTEGER = 1048576
			-- 1 MiB threshold: pause Reactor_in interest

	Out_buf_low_water: INTEGER = 262144
			-- 256 KiB threshold: resume Reactor_in interest

	Scratch_buffer_size: INTEGER = 16384
			-- 16 KiB scratch read chunk

	Idle_timeout_ms: NATURAL_64 = 60000
			-- 60 seconds of inactivity triggers close

	Timer_tick_interval_ms: INTEGER = 200
			-- Reactor wait timeout for periodic timer ticks

	Accept_budget_per_turn: INTEGER = 64
			-- Maximum non-blocking accepts per reactor poll turn

feature -- Buffer and Pool Configuration

	Event_size: INTEGER = 12
			-- Size in bytes of one native vr_reactor_event_t struct (fd: 4 + gen: 4 + flags: 4)

	Max_events: INTEGER = 1024
			-- Maximum number of events returned in one vr_reactor_wait call

	Default_worker_count: INTEGER = 4
			-- Default number of worker processors

	Default_backlog: INTEGER = 1024
			-- Default TCP listen queue backlog

	Metrics_size: INTEGER = 32
			-- Size in bytes of vr_reactor_metrics_t (4 * uint64_t)

end
