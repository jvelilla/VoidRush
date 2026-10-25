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

feature -- Execution

	start
			-- Launch supervised components in deterministic order.
		do
			supervisor.start_all
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
