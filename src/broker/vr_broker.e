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
			create ws_room_worker_mask.make (64)
			create sse_room_worker_mask.make (64)
			create sse_replay_buffers.make (64)
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
			clear_worker_subscriptions (a_index)
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

feature -- Room Membership Management (Invoked from Workers)

	update_ws_room_worker (a_room_id: INTEGER; a_worker_id: INTEGER; a_has_subscribers: BOOLEAN)
			-- Update presence bitmask for worker `a_worker_id` in WebSocket room `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_worker: a_worker_id >= 0 and a_worker_id < 64
		local
			mask, bit_mask: NATURAL_64
		do
			bit_mask := (1).to_natural_64 |<< a_worker_id
			if ws_room_worker_mask.has (a_room_id) then
				mask := ws_room_worker_mask.item (a_room_id)
			else
				mask := 0
			end
			if a_has_subscribers then
				mask := mask | bit_mask
				ws_room_worker_mask.force (mask, a_room_id)
			else
				mask := mask & bit_mask.bit_not
				if mask = 0 then
					ws_room_worker_mask.remove (a_room_id)
				else
					ws_room_worker_mask.force (mask, a_room_id)
				end
			end
		end

	update_sse_room_worker (a_room_id: INTEGER; a_worker_id: INTEGER; a_has_subscribers: BOOLEAN)
			-- Update presence bitmask for worker `a_worker_id` in SSE room `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_worker: a_worker_id >= 0 and a_worker_id < 64
		local
			mask, bit_mask: NATURAL_64
		do
			bit_mask := (1).to_natural_64 |<< a_worker_id
			if sse_room_worker_mask.has (a_room_id) then
				mask := sse_room_worker_mask.item (a_room_id)
			else
				mask := 0
			end
			if a_has_subscribers then
				mask := mask | bit_mask
				sse_room_worker_mask.force (mask, a_room_id)
			else
				mask := mask & bit_mask.bit_not
				if mask = 0 then
					sse_room_worker_mask.remove (a_room_id)
				else
					sse_room_worker_mask.force (mask, a_room_id)
				end
			end
		end

	clear_worker_subscriptions (a_worker_id: INTEGER)
			-- Clear presence bits for worker `a_worker_id` across all room masks.
		require
			valid_worker: a_worker_id >= 0 and a_worker_id < 64
		local
			bit_mask: NATURAL_64
			keys: ARRAYED_LIST [INTEGER]
			r_id: INTEGER
		do
			bit_mask := ((1).to_natural_64 |<< a_worker_id).bit_not
			create keys.make (ws_room_worker_mask.count)
			across ws_room_worker_mask as ws_ic loop
				keys.extend (@ws_ic.key)
			end
			across keys as k_ic loop
				r_id := k_ic
				if ws_room_worker_mask.has (r_id) then
					ws_room_worker_mask.force (ws_room_worker_mask.item (r_id) & bit_mask, r_id)
				end
			end

			create keys.make (sse_room_worker_mask.count)
			across sse_room_worker_mask as sse_ic loop
				keys.extend (@sse_ic.key)
			end
			across keys as k_ic loop
				r_id := k_ic
				if sse_room_worker_mask.has (r_id) then
					sse_room_worker_mask.force (sse_room_worker_mask.item (r_id) & bit_mask, r_id)
				end
			end
		end

feature -- Pub/Sub Commands (Invoked from Workers)

	publish (a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Broadcast message `a_msg_handle` across worker shards with active subscribers in `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
			active: not is_shutdown
		local
			i: INTEGER
			mask, bit_mask: NATURAL_64
		do
			if ws_room_worker_mask.has (a_room_id) then
				mask := ws_room_worker_mask.item (a_room_id)
			else
				mask := 0
			end
			from
				i := 0
			until
				i >= workers.count
			loop
				bit_mask := (1).to_natural_64 |<< i
				if (mask & bit_mask) /= 0 then
					dispatch_room_publish (workers.item (i), a_room_id, a_msg_handle)
				else
					-- Worker shard has no local subscribers: unref on its behalf to maintain atomic balance
					{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
				end
				i := i + 1
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	publish_sse (a_room_id: INTEGER; a_seq: NATURAL_64; a_msg_handle: POINTER)
			-- Broadcast SSE message `a_msg_handle` across worker shards with active subscribers in `a_room_id`,
			-- and record into per-room replay ring buffer.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
			active: not is_shutdown
		local
			i: INTEGER
			mask, bit_mask: NATURAL_64
			rl: ARRAYED_LIST [TUPLE [seq: NATURAL_64; msg: POINTER]]
			dropped: TUPLE [seq: NATURAL_64; msg: POINTER]
		do
			-- Replay ring-buffer maintenance
			if not sse_replay_buffers.has (a_room_id) then
				create rl.make (64)
				sse_replay_buffers.put (rl, a_room_id)
			end
			if attached sse_replay_buffers.item (a_room_id) as replay_list then
				{VR_REACTOR_BRIDGE}.c_ws_msg_ref (a_msg_handle)
				replay_list.extend ([a_seq, a_msg_handle])
				if replay_list.count > 1024 then
					dropped := replay_list.first
					replay_list.start
					replay_list.remove
					{VR_REACTOR_BRIDGE}.c_ws_msg_unref (dropped.msg)
				end
			end

			if sse_room_worker_mask.has (a_room_id) then
				mask := sse_room_worker_mask.item (a_room_id)
			else
				mask := 0
			end
			from
				i := 0
			until
				i >= workers.count
			loop
				bit_mask := (1).to_natural_64 |<< i
				if (mask & bit_mask) /= 0 then
					dispatch_sse_room_publish (workers.item (i), a_room_id, a_msg_handle)
				else
					-- Worker shard has no local subscribers: unref on its behalf to maintain atomic balance
					{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
				end
				i := i + 1
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_broker_fault (reactor_handle)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	replay_sse_for_worker (a_worker: separate VR_WORKER; a_fd, a_room_id: INTEGER; a_last_seq: NATURAL_64)
			-- Replay unexpired SSE messages with sequence > `a_last_seq` to newly connected client `a_fd`.
		require
			valid_worker: a_worker /= Void
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		local
			msg_h: POINTER
		do
			if sse_replay_buffers.has (a_room_id) and then attached sse_replay_buffers.item (a_room_id) as rl then
				across
					rl as item_ic
				loop
					if item_ic.seq > a_last_seq then
						msg_h := item_ic.msg
						{VR_REACTOR_BRIDGE}.c_ws_msg_ref (msg_h)
						dispatch_sse_replay_publish (a_worker, a_fd, msg_h)
					end
				end
			end
		end

	shutdown
			-- Mark broker terminated during reactor teardown barrier.
		local
			dropped: TUPLE [seq: NATURAL_64; msg: POINTER]
		do
			is_shutdown := True
			across sse_replay_buffers as buf_ic loop
				from
				until
					buf_ic.is_empty
				loop
					dropped := buf_ic.first
					buf_ic.start
					buf_ic.remove
					{VR_REACTOR_BRIDGE}.c_ws_msg_unref (dropped.msg)
				end
			end
			sse_replay_buffers.wipe_out
		ensure
			shutdown_set: is_shutdown
		end

feature {NONE} -- Implementation

	workers: SPECIAL [separate VR_WORKER]
			-- Worker processors to broadcast messages to

	ws_room_worker_mask: HASH_TABLE [NATURAL_64, INTEGER]
			-- Room subscriber presence bitmask across worker shards for WebSocket rooms

	sse_room_worker_mask: HASH_TABLE [NATURAL_64, INTEGER]
			-- Room subscriber presence bitmask across worker shards for SSE rooms

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

	dispatch_sse_replay_publish (a_worker: separate VR_WORKER; a_fd: INTEGER; a_msg_handle: POINTER)
			-- Asynchronous command passing replayed SSE message to worker.
		do
			a_worker.on_sse_replay_publish (a_fd, a_msg_handle)
		end

	sse_replay_buffers: HASH_TABLE [ARRAYED_LIST [TUPLE [seq: NATURAL_64; msg: POINTER]], INTEGER]
			-- Per-room bounded ring buffer of historical SSE messages for Last-Event-ID resumption

end
