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

feature -- Access

	is_shutdown: BOOLEAN
			-- Has broker been gracefully shut down?

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

end
