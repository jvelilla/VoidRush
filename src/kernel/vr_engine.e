note
	description: "Root application engine delegating lifecycle management to VR_SUPERVISOR"

class
	VR_ENGINE

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_port, a_worker_count: INTEGER)
			-- Initialize engine and prepare root supervisor.
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_workers: a_worker_count > 0 and a_worker_count <= Max_workers
		do
			port := a_port
			worker_count := a_worker_count
			create supervisor.make (a_port, a_worker_count)
		ensure
			port_set: port = a_port
			worker_count_set: worker_count = a_worker_count
			supervisor_created: supervisor /= Void
		end

feature -- Access

	port: INTEGER
			-- Listening TCP port

	worker_count: INTEGER
			-- Number of worker partitions

	supervisor: VR_SUPERVISOR
			-- Root supervisor coordinating child lifecycle

	reactor_handle: POINTER
			-- Direct pointer to native vr_reactor_t
		do
			Result := supervisor.reactor_handle
		end

feature -- Configuration

	set_restart_intensity (a_max_restarts: INTEGER; a_window_ms: NATURAL_64)
			-- Configure sliding-window restart intensity thresholds on root supervisor.
		require
			valid_max: a_max_restarts > 0
			valid_window: a_window_ms > 0
		do
			supervisor.set_restart_intensity (a_max_restarts, a_window_ms)
		end

	set_bind_host (a_host: STRING_8)
			-- Configure listening bind IP or hostname.
		require
			valid_host: a_host /= Void and then not a_host.is_empty
		do
			supervisor.set_bind_host (a_host)
		ensure
			host_set: attached supervisor.bind_host as h and then h.same_string (a_host)
		end

	enable_proxy_protocol_v2
			-- Configure workers to expect HAProxy PROXY v2 binary header on incoming connections.
		do
			supervisor.enable_proxy_protocol_v2
		ensure
			proxy_v2_enabled: supervisor.is_proxy_protocol_v2_enabled
		end

feature -- Execution

	start
			-- Launch supervised components in deterministic order.
		do
			supervisor.start_all
		end

	run
			-- Autonomous event loop until native shutdown signal is received.
		require
			running: supervisor.is_running
		do
			supervisor.run_loop
		end

	stop
			-- Request ordered graceful shutdown of all children.
		do
			supervisor.stop_all
		end

	shutdown
			-- Explicit teardown of all supervised components.
		do
			supervisor.stop_all
		end

	metrics: VR_METRICS
			-- Snapshot current metrics from native reactor.
		require
			valid_handle: reactor_handle /= default_pointer
		do
			create Result.make_from_pointer (reactor_handle)
		end

end
