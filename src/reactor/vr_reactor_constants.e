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

feature -- Protocol Sub-States (Phase 4)

	Proto_echo: INTEGER = 0
			-- Pure TCP Echo mode

	Proto_awaiting_proxy_v2: INTEGER = 1
			-- Awaiting 16+ byte binary PROXY protocol v2 header

	Proto_http_reading_headers: INTEGER = 2
			-- Reading HTTP/1.1 request line and headers

	Proto_http_reading_body: INTEGER = 3
			-- Reading HTTP/1.1 request body (Content-Length)

	Proto_ws_open: INTEGER = 4
			-- Full-duplex RFC 6455 WebSocket framing

	Proto_ws_closing: INTEGER = 5
			-- Awaiting or sending terminal WebSocket Close frame

	Proto_sse_open: INTEGER = 6
			-- Active WHATWG Server-Sent Events stream

feature -- WebSocket Opcodes (RFC 6455 §5.2)

	Ws_opcode_continuation: INTEGER = 0x0
	Ws_opcode_text: INTEGER = 0x1
	Ws_opcode_binary: INTEGER = 0x2
	Ws_opcode_close: INTEGER = 0x8
	Ws_opcode_ping: INTEGER = 0x9
	Ws_opcode_pong: INTEGER = 0xA

feature -- WebSocket Status Codes (RFC 6455 §7.4.1)

	Ws_close_normal: NATURAL_16 = 1000
	Ws_close_going_away: NATURAL_16 = 1001
	Ws_close_protocol_error: NATURAL_16 = 1002
	Ws_close_unsupported_data: NATURAL_16 = 1003
	Ws_close_message_too_big: NATURAL_16 = 1009
	Ws_close_internal_error: NATURAL_16 = 1011

feature -- WebSocket Limits

	Max_ws_message_size: INTEGER = 1048576
			-- 1 MiB maximum frame and fragmented message payload

	Max_ws_control_payload: INTEGER = 125
			-- RFC 6455 limit for Ping/Pong/Close payloads

	Room_default: INTEGER = 0
			-- Default broadcast room

feature -- HTTP Methods

	Method_unknown: INTEGER = 0
	Method_get: INTEGER = 1
	Method_post: INTEGER = 2
	Method_put: INTEGER = 3
	Method_delete: INTEGER = 4
	Method_head: INTEGER = 5
	Method_options: INTEGER = 6

feature -- HTTP Limits & Timeouts

	Max_header_size: INTEGER = 16384
			-- 16 KiB maximum request header section

	Default_body_limit: INTEGER = 10485760
			-- 10 MiB maximum request body size

	Request_timeout_ms: NATURAL_64 = 10000
			-- 10 seconds Slowloris request header deadline

feature -- Buffer and Pool Configuration

	Event_size: INTEGER = 12
			-- Size in bytes of one native vr_reactor_event_t struct (fd: 4 + gen: 4 + flags: 4)

	Max_events: INTEGER = 1024
			-- Maximum number of events returned in one vr_reactor_wait call

	Default_worker_count: INTEGER = 4
			-- Default number of worker processors

	Default_backlog: INTEGER = 1024
			-- Default TCP listen queue backlog

	Metrics_size: INTEGER = 136
			-- Size in bytes of vr_reactor_metrics_t (17 * uint64_t)

feature -- Phase 7 Supervision Child Kinds

	Child_reactor: INTEGER = 1
			-- C Epoll Reactor child component

	Child_broker: INTEGER = 2
			-- Pub/Sub Broker router child component

	Child_worker: INTEGER = 3
			-- Worker partition shard child component

feature -- Phase 7 Child Restart Policies

	Restart_permanent: INTEGER = 1
			-- Always restart child upon failure (e.g. Broker, Worker)

	Restart_transient: INTEGER = 2
			-- Restart only if abnormal exit / failure occurs (e.g. Reactor)

	Restart_temporary: INTEGER = 3
			-- Never restart child

feature -- Phase 7 Child Lifecycle States

	State_stopped: INTEGER = 0
			-- Component is stopped

	State_starting: INTEGER = 4
			-- Component is initializing

	State_running: INTEGER = 5
			-- Component is actively running

	State_stopping: INTEGER = 6
			-- Component is terminating

	State_failed: INTEGER = 7
			-- Component encountered unhandled fault

feature -- Phase 7 Supervision Limits & Timeouts

	Max_workers: INTEGER = 64
			-- Maximum number of worker shards supported by C control block

	Default_max_restarts: INTEGER = 3
			-- Maximum restarts within intensity window before fatal exit

	Default_intensity_window_ms: NATURAL_64 = 30000
			-- 30 seconds sliding window for restart rate limiting

	Default_liveness_timeout_ms: NATURAL_64 = 5000
			-- 5 seconds stale heartbeat threshold

	Supervisor_health_tick_ms: INTEGER = 250
			-- 250 ms supervisor health check interval

end
