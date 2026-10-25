note
	description: "Snapshot of native reactor and worker metrics"

class
	VR_METRICS

inherit
	VR_REACTOR_CONSTANTS
		redefine
			out
		end

create
	make_from_pointer

feature {NONE} -- Initialization

	make_from_pointer (a_handle: POINTER)
			-- Populate metric fields by querying native reactor C struct.
		require
			valid_handle: a_handle /= default_pointer
		do
			refresh (a_handle)
		end

feature -- Access

	active_connections: NATURAL_64
			-- Currently open connections tracked across all workers

	accepts_total: NATURAL_64
			-- Cumulative total of connections accepted

	events_total: NATURAL_64
			-- Cumulative count of epoll readiness events processed

	stale_events: NATURAL_64
			-- Count of events dropped due to generation token mismatch

	bytes_in_total: NATURAL_64
			-- Cumulative total bytes received from all connections

	bytes_out_total: NATURAL_64
			-- Cumulative total bytes sent across all connections

	idle_timeouts_total: NATURAL_64
			-- Cumulative total connections closed due to idle or Slowloris timeout

	ws_upgrades_total: NATURAL_64
			-- Cumulative total successful WebSocket HTTP 101 upgrades

	ws_frames_in_total: NATURAL_64
			-- Cumulative total inbound WebSocket frames parsed

	ws_frames_out_total: NATURAL_64
			-- Cumulative total outbound WebSocket frames transmitted

	ws_dropped_backpressure_total: NATURAL_64
			-- Cumulative total frames dropped due to subscriber buffer backpressure

	sse_streams_total: NATURAL_64
			-- Cumulative total active Server-Sent Events streams opened

	sse_events_out_total: NATURAL_64
			-- Cumulative total SSE events transmitted to clients

	sse_heartbeats_total: NATURAL_64
			-- Cumulative total SSE comment heartbeats transmitted

	sse_dropped_backpressure_total: NATURAL_64
			-- Cumulative total SSE events dropped due to client backpressure

	supervisor_restarts_total: NATURAL_64
			-- Cumulative total child restarts triggered by supervisor

	supervisor_isolated_faults_total: NATURAL_64
			-- Cumulative total connection faults isolated without child crash

feature -- Status Report

	refresh (a_handle: POINTER)
			-- Refresh metric fields from native reactor structure.
		require
			valid_handle: a_handle /= default_pointer
		local
			buf: MANAGED_POINTER
		do
			create buf.make (Metrics_size)
			{VR_REACTOR_BRIDGE}.c_reactor_get_metrics (a_handle, buf.item)
			active_connections := buf.read_natural_64 (0)
			accepts_total := buf.read_natural_64 (8)
			events_total := buf.read_natural_64 (16)
			stale_events := buf.read_natural_64 (24)
			bytes_in_total := buf.read_natural_64 (32)
			bytes_out_total := buf.read_natural_64 (40)
			idle_timeouts_total := buf.read_natural_64 (48)
			ws_upgrades_total := buf.read_natural_64 (56)
			ws_frames_in_total := buf.read_natural_64 (64)
			ws_frames_out_total := buf.read_natural_64 (72)
			ws_dropped_backpressure_total := buf.read_natural_64 (80)
			sse_streams_total := buf.read_natural_64 (88)
			sse_events_out_total := buf.read_natural_64 (96)
			sse_heartbeats_total := buf.read_natural_64 (104)
			sse_dropped_backpressure_total := buf.read_natural_64 (112)
			supervisor_restarts_total := buf.read_natural_64 (120)
			supervisor_isolated_faults_total := buf.read_natural_64 (128)
		end

feature -- Output

	out: STRING
			-- Human-readable metrics snapshot.
		do
			create Result.make (256)
			Result.append ("Metrics [accepts=" + accepts_total.out)
			Result.append (", active=" + active_connections.out)
			Result.append (", events=" + events_total.out)
			Result.append (", stale=" + stale_events.out)
			Result.append (", bytes_in=" + bytes_in_total.out)
			Result.append (", bytes_out=" + bytes_out_total.out)
			Result.append (", idle_drops=" + idle_timeouts_total.out)
			Result.append (", ws_upgrades=" + ws_upgrades_total.out)
			Result.append (", ws_in=" + ws_frames_in_total.out)
			Result.append (", ws_out=" + ws_frames_out_total.out)
			Result.append (", ws_dropped=" + ws_dropped_backpressure_total.out)
			Result.append (", sse_streams=" + sse_streams_total.out)
			Result.append (", sse_out=" + sse_events_out_total.out)
			Result.append (", sse_hb=" + sse_heartbeats_total.out)
			Result.append (", sse_dropped=" + sse_dropped_backpressure_total.out)
			Result.append (", sup_restarts=" + supervisor_restarts_total.out)
			Result.append (", sup_faults=" + supervisor_isolated_faults_total.out + "]")
		end

end
