note
	description: "Root application engine coordinating bootstrap and graceful shutdown"

class
	VR_ENGINE

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_port, a_worker_count: INTEGER)
			-- Initialize engine, launch reactor processor, and retain C handle.
		require
			valid_port: a_port > 0 and a_port <= 65535
			valid_workers: a_worker_count > 0
		local
			r: separate VR_REACTOR
		do
			port := a_port
			worker_count := a_worker_count
			create r.make (a_port, a_worker_count)
			reactor := r
			-- Retrieve native pointer synchronously before launching loop
			reactor_handle := get_reactor_handle (r)
		ensure
			reactor_created: reactor /= Void
			handle_captured: reactor_handle /= default_pointer
		end

feature -- Access

	port: INTEGER
			-- Listening TCP port

	worker_count: INTEGER
			-- Number of worker partitions

	reactor_handle: POINTER
			-- Direct pointer to native vr_reactor_t

feature -- Execution

	start
			-- Launch reactor run loop asynchronously.
		require
			valid_handle: reactor_handle /= default_pointer
		do
			launch_loop (reactor)
		end

	stop
			-- Signal native reactor shutdown via lock-free atomic FFI call.
		require
			valid_handle: reactor_handle /= default_pointer
		do
			{VR_REACTOR_BRIDGE}.c_reactor_request_shutdown (reactor_handle)
		end

	shutdown
			-- Explicit teardown of reactor and all workers.
		require
			valid_handle: reactor_handle /= default_pointer
		do
			stop
			shutdown_reactor (reactor)
		end

	metrics: VR_METRICS
			-- Snapshot current metrics from native reactor.
		require
			valid_handle: reactor_handle /= default_pointer
		do
			create Result.make_from_pointer (reactor_handle)
		end

feature {NONE} -- Implementation

	reactor: separate VR_REACTOR

	get_reactor_handle (a_reactor: separate VR_REACTOR): POINTER
			-- Synchronous query retrieving native handle.
		do
			Result := a_reactor.native_handle
		end

	launch_loop (a_reactor: separate VR_REACTOR)
			-- Asynchronous command launching reactor main loop.
		do
			a_reactor.run_loop
		end

	shutdown_reactor (a_reactor: separate VR_REACTOR)
			-- Asynchronous command shutting down reactor.
		do
			a_reactor.shutdown
		end

end
