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
			create worker_conns.make_empty
			refresh (a_handle)
		ensure
			worker_conns_valid: worker_conns.count = Max_workers
			active_workers_valid: active_worker_count >= 1 and active_worker_count <= Max_workers
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

	worker_conns: ARRAY [NATURAL_32]
			-- Active connections per worker shard (indexed 0 .. Max_workers - 1)

	active_worker_count: INTEGER
			-- Count of worker shards to expose in metrics

feature -- Status Report

	refresh (a_handle: POINTER)
			-- Refresh metric fields from native reactor structure.
		require
			valid_handle: a_handle /= default_pointer
		local
			buf: MANAGED_POINTER
			i: INTEGER
			w_conns: NATURAL_32
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

			-- Snapshot per-worker atomic connection loads
			create worker_conns.make_filled (0, 0, Max_workers - 1)
			active_worker_count := 4
			from
				i := 0
			until
				i >= Max_workers
			loop
				w_conns := {VR_REACTOR_BRIDGE}.c_reactor_get_worker_conns (a_handle, i)
				worker_conns.put (w_conns, i)
				if (w_conns > 0 or {VR_REACTOR_BRIDGE}.c_reactor_get_worker_beat (a_handle, i) > 0) and (i + 1 > active_worker_count) then
					active_worker_count := i + 1
				end
				i := i + 1
			end
		end

feature -- Output

	to_prometheus_format: STRING_8
			-- Prometheus text exposition format.
		local
			i: INTEGER
		do
			create Result.make (2048)
			Result.append ("# HELP vr_connections_active Currently active connections%N")
			Result.append ("# TYPE vr_connections_active gauge%N")
			Result.append ("vr_connections_active " + active_connections.out + "%N")
			Result.append ("# HELP vr_worker_connections_active Active connections assigned per worker shard%N")
			Result.append ("# TYPE vr_worker_connections_active gauge%N")
			from
				i := 0
			until
				i >= active_worker_count
			loop
				Result.append ("vr_worker_connections_active{worker=%"" + i.out + "%"} " + worker_conns.item (i).out + "%N")
				i := i + 1
			end
			Result.append ("# HELP vr_accepts_total Cumulative accepted connections%N")
			Result.append ("# TYPE vr_accepts_total counter%N")
			Result.append ("vr_accepts_total " + accepts_total.out + "%N")
			Result.append ("# HELP vr_events_total Epoll readiness events processed%N")
			Result.append ("# TYPE vr_events_total counter%N")
			Result.append ("vr_events_total " + events_total.out + "%N")
			Result.append ("# HELP vr_stale_events_total Events dropped due to generation token mismatch%N")
			Result.append ("# TYPE vr_stale_events_total counter%N")
			Result.append ("vr_stale_events_total " + stale_events.out + "%N")
			Result.append ("# HELP vr_bytes_in_total Total bytes received%N")
			Result.append ("# TYPE vr_bytes_in_total counter%N")
			Result.append ("vr_bytes_in_total " + bytes_in_total.out + "%N")
			Result.append ("# HELP vr_bytes_out_total Total bytes sent%N")
			Result.append ("# TYPE vr_bytes_out_total counter%N")
			Result.append ("vr_bytes_out_total " + bytes_out_total.out + "%N")
			Result.append ("# HELP vr_ws_upgrades_total Total successful WebSocket upgrades%N")
			Result.append ("# TYPE vr_ws_upgrades_total counter%N")
			Result.append ("vr_ws_upgrades_total " + ws_upgrades_total.out + "%N")
			Result.append ("# HELP vr_ws_frames_in_total Total inbound WebSocket frames%N")
			Result.append ("# TYPE vr_ws_frames_in_total counter%N")
			Result.append ("vr_ws_frames_in_total " + ws_frames_in_total.out + "%N")
			Result.append ("# HELP vr_ws_frames_out_total Total outbound WebSocket frames%N")
			Result.append ("# TYPE vr_ws_frames_out_total counter%N")
			Result.append ("vr_ws_frames_out_total " + ws_frames_out_total.out + "%N")
			Result.append ("# HELP vr_sse_streams_total Total SSE streams created%N")
			Result.append ("# TYPE vr_sse_streams_total counter%N")
			Result.append ("vr_sse_streams_total " + sse_streams_total.out + "%N")
			Result.append ("# HELP vr_sse_events_out_total Total SSE events sent%N")
			Result.append ("# TYPE vr_sse_events_out_total counter%N")
			Result.append ("vr_sse_events_out_total " + sse_events_out_total.out + "%N")
			Result.append ("# HELP vr_sse_heartbeats_total Total SSE heartbeats sent%N")
			Result.append ("# TYPE vr_sse_heartbeats_total counter%N")
			Result.append ("vr_sse_heartbeats_total " + sse_heartbeats_total.out + "%N")
			Result.append ("# HELP vr_supervisor_restarts_total Cumulative child supervisor restarts%N")
			Result.append ("# TYPE vr_supervisor_restarts_total counter%N")
			Result.append ("vr_supervisor_restarts_total " + supervisor_restarts_total.out + "%N")
			Result.append ("# HELP vr_supervisor_isolated_faults_total Rescued single-connection faults%N")
			Result.append ("# TYPE vr_supervisor_isolated_faults_total counter%N")
			Result.append ("vr_supervisor_isolated_faults_total " + supervisor_isolated_faults_total.out + "%N")
		end

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

invariant
	valid_worker_conns: worker_conns.count = Max_workers
	valid_active_workers: active_worker_count >= 1 and active_worker_count <= Max_workers

end
