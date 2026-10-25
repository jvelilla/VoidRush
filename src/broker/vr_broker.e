note
	description: "Separate SCOOP pub/sub broker distributing pre-framed messages to workers"

class
	VR_BROKER

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Initialize broker with empty worker list.
		do
			create workers.make_empty (Default_worker_count)
			is_shutdown := False
		ensure
			empty_workers: workers.count = 0
			not_shutdown: not is_shutdown
		end

feature -- Configuration

	register_worker (a_worker: separate VR_WORKER)
			-- Add worker partition to broadcast fan-out list.
		require
			valid_worker: a_worker /= Void
			not_shutdown: not is_shutdown
		do
			workers.extend (a_worker)
		ensure
			worker_added: workers.count = old workers.count + 1
		end

	replace_worker (a_index: INTEGER; a_worker: separate VR_WORKER)
			-- Hot-swap worker slot `a_index` in fan-out array following worker restart.
		require
			valid_index: a_index >= 0 and a_index < worker_count
			valid_worker: a_worker /= Void
			not_shutdown: not is_shutdown
		do
			workers.put (a_worker, a_index)
		ensure
			worker_replaced: workers.item (a_index) = a_worker
		end

	set_reactor_handle (a_handle: POINTER)
			-- Set native reactor handle for heartbeat and fault signaling.
		require
			valid_handle: a_handle /= default_pointer
		do
			reactor_handle := a_handle
		ensure
			handle_set: reactor_handle = a_handle
		end

feature -- Access

	worker_count: INTEGER
			-- Total number of registered worker shards
		do
			Result := workers.count
		end

	is_shutdown: BOOLEAN
			-- Has broker been gracefully shut down?

	reactor_handle: POINTER
			-- Direct pointer to native vr_reactor_t

feature -- Supervision Commands

	on_supervisor_tick (a_now_ms: NATURAL_64)
			-- Periodic supervisor heartbeat tick recorded into C control block.
		require
			valid_time: a_now_ms > 0
		do
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_beat_broker (reactor_handle, a_now_ms)
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

feature -- Pub/Sub Commands (Invoked from Workers)

	publish (a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Broadcast message `a_msg_handle` across all worker shards for `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
			active: not is_shutdown
		local
			i: INTEGER
		do
			from
				i := 0
			until
				i >= workers.count
			loop
				dispatch_room_publish (workers.item (i), a_room_id, a_msg_handle)
				i := i + 1
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	publish_sse (a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Broadcast SSE message `a_msg_handle` across all worker shards for `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
			active: not is_shutdown
		local
			i: INTEGER
		do
			from
				i := 0
			until
				i >= workers.count
			loop
				dispatch_sse_room_publish (workers.item (i), a_room_id, a_msg_handle)
				i := i + 1
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	shutdown
			-- Mark broker terminated during reactor teardown barrier.
		do
			is_shutdown := True
		ensure
			shutdown_set: is_shutdown
		end

feature {NONE} -- Implementation

	workers: SPECIAL [separate VR_WORKER]
			-- Worker processors to broadcast messages to

	dispatch_room_publish (a_worker: separate VR_WORKER; a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Asynchronous command passing basic value types to controlled worker.
		do
			a_worker.on_room_publish (a_room_id, a_msg_handle)
		end

	dispatch_sse_room_publish (a_worker: separate VR_WORKER; a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Asynchronous command passing basic value types to controlled worker.
		do
			a_worker.on_sse_room_publish (a_room_id, a_msg_handle)
		end

end
