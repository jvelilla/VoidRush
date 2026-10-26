note
	description: "Worker processor managing a partition of connections, non-blocking I/O turn, and idle sweeps"

class
	VR_WORKER

inherit
	VR_REACTOR_CONSTANTS
	VR_SSE_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_id: INTEGER; a_reactor_handle: POINTER; a_broker: separate VR_BROKER; a_total_workers: INTEGER)
			-- Initialize worker `a_id` linked to native reactor `a_reactor_handle` and shared `a_broker`.
		require
			valid_id: a_id >= 0
			valid_handle: a_reactor_handle /= default_pointer
			valid_broker: a_broker /= Void
			valid_total: a_total_workers > 0
		local
			i: INTEGER
		do
			worker_id := a_id
			reactor_handle := a_reactor_handle
			broker := a_broker
			total_workers := a_total_workers
			create connections.make (1024)
			create rooms.make (64)
			create sse_rooms.make (64)
			create sse_active_fds.make (64)
			create expired_fds.make (64)
			create scratch_buffer.make (Scratch_buffer_size)
			create sse_heartbeat_buffer.make (Sse_heartbeat_comment.count)
			from
				i := 1
			until
				i > Sse_heartbeat_comment.count
			loop
				sse_heartbeat_buffer.put_character (Sse_heartbeat_comment.item (i), i - 1)
				i := i + 1
			end
			current_fd := -1
			is_shutdown_complete := False
			protocol := Proto_http_reading_headers
		ensure
			id_set: worker_id = a_id
			handle_set: reactor_handle = a_reactor_handle
			not_shutdown: not is_shutdown_complete
			empty_connections: connections.is_empty
			default_proto: protocol = Proto_http_reading_headers
		end

feature -- Access

	worker_id: INTEGER
			-- Unique partition index

	current_fd: INTEGER
			-- FD currently being processed during on_socket_event for fault isolation

	reactor_handle: POINTER
			-- Pointer to native vr_reactor_t instance

	broker: separate VR_BROKER
			-- Reference to shared separate Pub/Sub broker

	total_workers: INTEGER
			-- Total number of worker shards in reactor pool

	rooms: HASH_TABLE [HASH_TABLE [BOOLEAN, INTEGER], INTEGER]
			-- Worker-local subscription directory: room_id -> (client FD -> True)

	sse_rooms: HASH_TABLE [HASH_TABLE [BOOLEAN, INTEGER], INTEGER]
			-- Worker-local SSE subscription directory: room_id -> (client FD -> True)

	sse_active_fds: HASH_TABLE [BOOLEAN, INTEGER]
			-- Fast index of active SSE connections on this worker shard for O(sse) heartbeat ticks

	is_shutdown_complete: BOOLEAN
			-- Has worker cleanly closed all connections during shutdown?

	protocol: INTEGER
			-- Inbound protocol state for newly registered connections

	connection_count: INTEGER
			-- Number of currently tracked active connections
		do
			Result := connections.count
		end

feature -- Configuration

	set_protocol (a_proto: INTEGER)
			-- Configure protocol state for subsequent client registrations.
		require
			valid_proto: a_proto >= Proto_echo and a_proto <= Proto_http_reading_body
		do
			protocol := a_proto
		ensure
			proto_set: protocol = a_proto
		end

	set_broker (a_broker: separate VR_BROKER)
			-- Update shared broker reference upon supervisor broker restart.
		require
			valid_broker: a_broker /= Void
		do
			broker := a_broker
			across rooms as r_ic loop
				if not r_ic.is_empty then
					dispatch_broker_update_ws_room (broker, @r_ic.key, worker_id, True)
				end
			end
			across sse_rooms as sse_ic loop
				if not sse_ic.is_empty then
					dispatch_broker_update_sse_room (broker, @sse_ic.key, worker_id, True)
				end
			end
		ensure
			broker_set: broker = a_broker
		end

feature -- Asynchronous Commands (Invoked from VR_REACTOR)

	register_connection (a_fd: INTEGER; a_generation: NATURAL_32)
			-- Insert connection into local map, then arm in native reactor.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		local
			conn: VR_CONNECTION
			res: INTEGER
			now_ms: NATURAL_64
		do
			if connections.has (a_fd) then
				-- Defensive cleanup if OS recycled FD before old close was processed
				close_connection (a_fd)
			end
			now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
			create conn.make_with_proto (a_fd, a_generation, now_ms, protocol)
			conn.set_reactor_handle (reactor_handle)
			conn.set_broadcast_handler (agent publish_broadcast)
			conn.set_room_join_handler (agent join_room)
			conn.set_sse_broadcast_handler (agent publish_sse_broadcast_from_slice)
			conn.set_sse_room_join_handler (agent join_sse_room)
			connections.force (conn, a_fd)
			{VR_REACTOR_BRIDGE}.c_metric_inc_open (reactor_handle)
			{VR_REACTOR_BRIDGE}.c_reactor_set_fd_owner (reactor_handle, a_fd, worker_id.to_integer_16)

			-- Configure TCP User Timeout (30s)
			res := {VR_REACTOR_BRIDGE}.c_reactor_set_tcp_user_timeout (a_fd, 30000)

			-- Worker-exclusive registration to epoll
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (reactor_handle, a_fd, Reactor_in, a_generation)
			if res = 0 then
				conn.set_armed_flags (Reactor_in)
			else
				close_connection (a_fd)
			end
		ensure
			connection_tracked_or_cleaned: connections.has (a_fd) implies (attached connections.item (a_fd) as c and then c.generation = a_generation)
		end

	on_socket_event (a_fd: INTEGER; a_generation, a_flags: NATURAL_32)
			-- Process 7-step readiness turn for socket `a_fd`.
		require
			valid_fd: a_fd >= 0
		local
			now_ms: NATURAL_64
			read_turn, write_turn, chunk, n: INTEGER
			read_done, write_done: BOOLEAN
		do
			current_fd := a_fd
			if is_fault_injected then
				is_fault_injected := False
				check test_isolated_fault: False end
			end
			if attached connections.item (a_fd) as conn and then conn.matches (a_generation) then
				conn.set_armed_flags (0)
				now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms

				-- Step 2: Fatal error check (immediate abort)
				if (a_flags & Reactor_err) /= 0 then
					close_connection (a_fd)
				else
					-- Step 3: Inbound Read Loop (Executed BEFORE checking Reactor_hup)
					if (a_flags & Reactor_in) /= 0 and conn.state = State_open and not conn.paused_in then
						from
							read_turn := 0
							read_done := False
						until
							read_done or read_turn >= Read_budget_per_turn
						loop
							chunk := (Read_budget_per_turn - read_turn).min (Scratch_buffer_size)
							conn.ensure_inbound_capacity (chunk)
							n := {VR_REACTOR_BRIDGE}.c_reactor_read (a_fd, conn.in_write_pointer, chunk)
							if n > 0 then
								read_turn := read_turn + n
								{VR_REACTOR_BRIDGE}.c_metric_add_bytes_in (reactor_handle, n.to_natural_64)
								conn.advance_in_write (n, now_ms)
								if conn.out_buf_bytes >= Out_buf_high_water then
									conn.set_paused_in (True)
									read_done := True
								end
							elseif n = Reactor_io_eof then
								conn.mark_closing
								read_done := True
							elseif n = Reactor_io_would_block then
								read_done := True
							else -- Reactor_io_error (-2)
								close_connection (a_fd)
								read_done := True
							end
						end
					end

					-- Step 4: Half-closed check (latching Closing state after reading inbound data)
					if connections.has (a_fd) and then (a_flags & Reactor_hup) /= 0 and conn.state = State_open then
						conn.mark_closing
					end

					-- Step 5: Outbound Write Loop (flushing out_buf under write budget)
					if connections.has (a_fd) then
						from
							write_turn := 0
							write_done := False
						until
							write_done or conn.out_buf_bytes = 0 or write_turn >= Write_budget_per_turn
						loop
							chunk := conn.out_buf_bytes.min (Write_budget_per_turn - write_turn)
							n := {VR_REACTOR_BRIDGE}.c_reactor_write (a_fd, conn.read_pointer, chunk)
							if n > 0 then
								write_turn := write_turn + n
								{VR_REACTOR_BRIDGE}.c_metric_add_bytes_out (reactor_handle, n.to_natural_64)
								conn.advance_read (n)
								conn.touch (now_ms)
							elseif n = Reactor_io_would_block then
								write_done := True
							else -- Reactor_io_error (-2)
								close_connection (a_fd)
								write_done := True
							end
						end
					end

					-- Step 6: Update flow-control hysteresis latch
					if connections.has (a_fd) then
						if conn.out_buf_bytes >= Out_buf_high_water then
							conn.set_paused_in (True)
						elseif conn.out_buf_bytes <= Out_buf_low_water then
							conn.set_paused_in (False)
						end

						-- Step 7: Final state disposition
						if conn.state = State_closing and conn.out_buf_bytes = 0 then
							close_connection (a_fd)
						else
							sync_interest (conn)
						end
					end
				end
			else
				-- Stale event for an obsolete generation or unregistered socket
				{VR_REACTOR_BRIDGE}.c_metric_inc_stale (reactor_handle)
			end
			current_fd := -1
		rescue
			if current_fd >= 0 then
				close_connection (current_fd)
				current_fd := -1
				if reactor_handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_metric_inc_isolated_faults (reactor_handle)
				end
				retry
			else
				if reactor_handle /= default_pointer then
					{VR_REACTOR_BRIDGE}.c_reactor_report_worker_fault (reactor_handle, worker_id)
					{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
				end
			end
		end

	on_timer_tick (a_now_ms: NATURAL_64)
			-- Worker-local idle sweep and SSE heartbeat dispatch.
		require
			valid_time: a_now_ms > 0
		local
			target_fd: INTEGER
		do
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_beat_worker (reactor_handle, worker_id, a_now_ms)
			end

			-- Fast SSE Heartbeat Sweep over dedicated SSE index (Phase C)
			across
				sse_active_fds as sse_ic
			loop
				target_fd := @sse_ic.key
				if attached connections.item (target_fd) as ic and then ic.proto_state = Proto_sse_open then
					if a_now_ms - ic.last_sse_heartbeat_ms >= Sse_heartbeat_interval_ms then
						deliver_frame_to_connection (ic, sse_heartbeat_buffer.item, Sse_heartbeat_comment.count)
						ic.set_last_sse_heartbeat_ms (a_now_ms)
						if reactor_handle /= default_pointer then
							{VR_REACTOR_BRIDGE}.c_metric_inc_sse_heartbeats (reactor_handle)
						end
					end
				end
			end

			-- General Inactivity and Slowloris Sweep using pre-allocated list (Phase A)
			expired_fds.wipe_out
			across
				connections as ic
			loop
				if ic.is_expired (a_now_ms) then
					expired_fds.extend (@ic.key)
				end
			end
			across
				expired_fds as fd_ic
			loop
				{VR_REACTOR_BRIDGE}.c_metric_inc_idle_timeouts (reactor_handle)
				close_connection (fd_ic)
			end
		rescue
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_worker_fault (reactor_handle, worker_id)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	shutdown
			-- Gracefully close all sockets assigned to this worker.
		local
			fds: ARRAYED_LIST [INTEGER]
		do
			create fds.make (connections.count)
			across
				connections as ic
			loop
				fds.extend (@ic.key)
			end
			across
				fds as fd_ic
			loop
				close_connection (fd_ic)
			end
			is_shutdown_complete := True
		ensure
			empty_connections: connections.is_empty
			shutdown_flag_set: is_shutdown_complete
		end

feature -- Room & Pub/Sub Management (Worker-Local & Fast-Path)

	join_room (a_fd, a_room_id: INTEGER)
			-- Subscribe connection `a_fd` to `a_room_id` locally.
		require
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		local
			set: HASH_TABLE [BOOLEAN, INTEGER]
			was_first: BOOLEAN
		do
			if not rooms.has (a_room_id) then
				create set.make (64)
				rooms.put (set, a_room_id)
				was_first := True
			end
			if attached rooms.item (a_room_id) as s then
				if s.is_empty then
					was_first := True
				end
				s.force (True, a_fd)
				if attached connections.item (a_fd) as conn then
					conn.register_room (a_room_id)
				end
			end
			if was_first then
				dispatch_broker_update_ws_room (broker, a_room_id, worker_id, True)
			end
		ensure
			room_has_fd: attached rooms.item (a_room_id) as s and then s.has (a_fd)
		end

	leave_room (a_fd, a_room_id: INTEGER)
			-- Unsubscribe connection `a_fd` from `a_room_id` locally.
		require
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		local
			became_empty: BOOLEAN
		do
			if attached rooms.item (a_room_id) as s then
				s.remove (a_fd)
				if s.is_empty then
					rooms.remove (a_room_id)
					became_empty := True
				end
			end
			if attached connections.item (a_fd) as conn then
				conn.unregister_room (a_room_id)
			end
			if became_empty then
				dispatch_broker_update_ws_room (broker, a_room_id, worker_id, False)
			end
		end

	publish_broadcast (a_room_id, a_opcode: INTEGER; a_payload: POINTER; a_len: INTEGER)
			-- Build C ref-counted message and asynchronously pass to broker.
		require
			valid_room: a_room_id >= 0
			valid_len: a_len >= 0
		local
			msg_ptr: POINTER
		do
			msg_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_create (a_opcode, a_payload, a_len, total_workers)
			if msg_ptr /= default_pointer then
				dispatch_broker_publish (broker, a_room_id, msg_ptr)
			end
		end

	on_room_publish (a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Fan out pre-framed broadcast message to all local subscribers of `a_room_id`.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
		local
			data_ptr: POINTER
			total_len, target_fd: INTEGER
		do
			if attached rooms.item (a_room_id) as subscriber_list then
				data_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_data (a_msg_handle)
				total_len := {VR_REACTOR_BRIDGE}.c_ws_msg_size (a_msg_handle)

				across
					subscriber_list as sub_ic
				loop
					target_fd := @sub_ic.key
					if attached connections.item (target_fd) as conn and then conn.proto_state = Proto_ws_open then
						deliver_frame_to_connection (conn, data_ptr, total_len)
					end
				end
			end
			-- Decrement reference count; last worker automatically frees C memory
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
		rescue
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_worker_fault (reactor_handle, worker_id)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	deliver_frame_to_connection (a_conn: VR_CONNECTION; a_ptr: POINTER; a_len: INTEGER)
			-- Deliver frame using direct non-blocking write fast-path.
		require
			valid_conn: a_conn /= Void
			valid_ptr: a_ptr /= default_pointer
			valid_len: a_len > 0
		local
			n: INTEGER
			now_ms: NATURAL_64
		do
			if a_conn.paused_in then
				if a_conn.proto_state = Proto_sse_open then
					{VR_REACTOR_BRIDGE}.c_metric_inc_sse_dropped (reactor_handle)
				else
					{VR_REACTOR_BRIDGE}.c_metric_inc_ws_dropped (reactor_handle)
				end
			elseif a_conn.out_buf_bytes = 0 then
				-- Fast Path: Socket idle, write directly to kernel
				n := {VR_REACTOR_BRIDGE}.c_reactor_write (a_conn.fd, a_ptr, a_len)
				now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
				if n = a_len then
					a_conn.touch (now_ms)
					{VR_REACTOR_BRIDGE}.c_metric_add_bytes_out (reactor_handle, n.to_natural_64)
					if a_conn.proto_state = Proto_sse_open then
						{VR_REACTOR_BRIDGE}.c_metric_inc_sse_events_out (reactor_handle)
					else
						{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_out (reactor_handle)
					end
				elseif n > 0 then
					a_conn.append_outbound (a_ptr + n, a_len - n)
					a_conn.touch (now_ms)
					{VR_REACTOR_BRIDGE}.c_metric_add_bytes_out (reactor_handle, n.to_natural_64)
					sync_interest (a_conn)
				elseif n = Reactor_io_would_block then
					a_conn.append_outbound (a_ptr, a_len)
					sync_interest (a_conn)
				else -- Reactor_io_error (-2)
					close_connection (a_conn.fd)
				end
			else
				a_conn.append_outbound (a_ptr, a_len)
				if a_conn.out_buf_bytes >= Out_buf_high_water then
					a_conn.set_paused_in (True)
				end
				sync_interest (a_conn)
			end
		end

	join_sse_room (a_fd, a_room_id: INTEGER; a_last_event_id: detachable STRING_8)
			-- Subscribe connection `a_fd` to local SSE `a_room_id` and trigger replay if Last-Event-ID is present.
		require
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		local
			set: HASH_TABLE [BOOLEAN, INTEGER]
			was_first: BOOLEAN
			last_seq: NATURAL_64
		do
			sse_active_fds.force (True, a_fd)
			if not sse_rooms.has (a_room_id) then
				create set.make (64)
				sse_rooms.put (set, a_room_id)
				was_first := True
			end
			if attached sse_rooms.item (a_room_id) as s then
				if s.is_empty then
					was_first := True
				end
				s.force (True, a_fd)
				if attached connections.item (a_fd) as conn then
					conn.register_room (a_room_id)
				end
			end
			if was_first then
				dispatch_broker_update_sse_room (broker, a_room_id, worker_id, True)
			end
			if attached a_last_event_id as lid and then not lid.is_empty and then lid.is_natural_64 then
				last_seq := lid.to_natural_64
				dispatch_broker_replay_sse (broker, Current, a_fd, a_room_id, last_seq)
			end
		ensure
			room_has_fd: attached sse_rooms.item (a_room_id) as s and then s.has (a_fd)
		end

	leave_sse_room (a_fd, a_room_id: INTEGER)
			-- Unsubscribe connection `a_fd` from local SSE `a_room_id`.
		require
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		local
			became_empty: BOOLEAN
		do
			if attached sse_rooms.item (a_room_id) as s then
				s.remove (a_fd)
				if s.is_empty then
					sse_rooms.remove (a_room_id)
					became_empty := True
				end
			end
			if attached connections.item (a_fd) as conn then
				conn.unregister_room (a_room_id)
			end
			if became_empty then
				dispatch_broker_update_sse_room (broker, a_room_id, worker_id, False)
			end
		end

	publish_sse_broadcast (a_room_id: INTEGER; a_id, a_event, a_data: STRING_8)
			-- Format SSE broadcast message once in C and asynchronously pass to broker.
		require
			valid_room: a_room_id >= 0
			valid_data: a_data /= Void
		local
			msg_ptr: POINTER
			c_id, c_ev, c_dt: ANY
			seq: NATURAL_64
			final_id: STRING_8
		do
			if a_id /= Void and then not a_id.is_empty then
				final_id := a_id
				if a_id.is_natural_64 then
					seq := a_id.to_natural_64
				else
					seq := {VR_REACTOR_BRIDGE}.c_reactor_next_room_seq (reactor_handle, a_room_id)
				end
			else
				seq := {VR_REACTOR_BRIDGE}.c_reactor_next_room_seq (reactor_handle, a_room_id)
				final_id := seq.out
			end
			c_id := final_id.to_c
			c_ev := a_event.to_c
			c_dt := a_data.to_c
			msg_ptr := {VR_REACTOR_BRIDGE}.c_sse_msg_create ($c_id, final_id.count, $c_ev, a_event.count, $c_dt, a_data.count, total_workers)
			if msg_ptr /= default_pointer then
				dispatch_broker_publish_sse (broker, a_room_id, seq, msg_ptr)
			end
		end

	publish_sse_broadcast_from_slice (a_room_id: INTEGER; a_payload: POINTER; a_len: INTEGER)
			-- Format SSE message from raw memory slice with monotonic sequence ID and dispatch to broker.
		require
			valid_room: a_room_id >= 0
			valid_len: a_len >= 0
		local
			msg_ptr: POINTER
			c_empty, c_id: ANY
			empty_str, id_str: STRING_8
			seq: NATURAL_64
		do
			empty_str := ""
			c_empty := empty_str.to_c
			seq := {VR_REACTOR_BRIDGE}.c_reactor_next_room_seq (reactor_handle, a_room_id)
			id_str := seq.out
			c_id := id_str.to_c
			msg_ptr := {VR_REACTOR_BRIDGE}.c_sse_msg_create ($c_id, id_str.count, $c_empty, 0, a_payload, a_len, total_workers)
			if msg_ptr /= default_pointer then
				dispatch_broker_publish_sse (broker, a_room_id, seq, msg_ptr)
			end
		end

	on_sse_room_publish (a_room_id: INTEGER; a_msg_handle: POINTER)
			-- Fan out pre-formatted SSE broadcast to all local SSE subscribers.
		require
			valid_room: a_room_id >= 0
			valid_msg: a_msg_handle /= default_pointer
		local
			data_ptr: POINTER
			total_len, target_fd: INTEGER
		do
			if attached sse_rooms.item (a_room_id) as subscriber_list then
				data_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_data (a_msg_handle)
				total_len := {VR_REACTOR_BRIDGE}.c_ws_msg_size (a_msg_handle)

				across
					subscriber_list as sub_ic
				loop
					target_fd := @sub_ic.key
					if attached connections.item (target_fd) as conn and then conn.proto_state = Proto_sse_open then
						deliver_frame_to_connection (conn, data_ptr, total_len)
					end
				end
			end
			-- Decrement reference count; last worker frees memory atomically
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
		rescue
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
			if reactor_handle /= default_pointer then
				{VR_REACTOR_BRIDGE}.c_reactor_report_worker_fault (reactor_handle, worker_id)
				{VR_REACTOR_BRIDGE}.c_reactor_wake (reactor_handle)
			end
		end

	on_sse_replay_publish (a_fd: INTEGER; a_msg_handle: POINTER)
			-- Deliver replayed SSE event to targeted connection and release buffer.
		require
			valid_fd: a_fd >= 0
			valid_msg: a_msg_handle /= default_pointer
		local
			data_ptr: POINTER
			total_len: INTEGER
		do
			if attached connections.item (a_fd) as conn and then conn.proto_state = Proto_sse_open then
				data_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_data (a_msg_handle)
				total_len := {VR_REACTOR_BRIDGE}.c_ws_msg_size (a_msg_handle)
				deliver_frame_to_connection (conn, data_ptr, total_len)
			end
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
		rescue
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
		end

feature {NONE} -- Epoll Interest Hysteresis

	sync_interest (a_conn: VR_CONNECTION)
			-- Synchronize epoll interest with connection interest_flags if transitioned.
		require
			valid_conn: a_conn /= Void
		local
			res: INTEGER
		do
			if a_conn.interest_flags /= a_conn.armed_flags then
				if a_conn.interest_flags /= 0 then
					res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_conn.fd, a_conn.interest_flags, a_conn.generation)
					if res = 0 then
						a_conn.set_armed_flags (a_conn.interest_flags)
					else
						close_connection (a_conn.fd)
					end
				end
			end
		end

feature {NONE} -- SCOOP Broker Dispatch

	dispatch_broker_publish (a_broker: separate VR_BROKER; a_room_id: INTEGER; a_msg_ptr: POINTER)
			-- Asynchronous command to controlled separate broker.
		do
			a_broker.publish (a_room_id, a_msg_ptr)
		end

	dispatch_broker_publish_sse (a_broker: separate VR_BROKER; a_room_id: INTEGER; a_seq: NATURAL_64; a_msg_ptr: POINTER)
			-- Asynchronous command to controlled separate broker for SSE.
		do
			a_broker.publish_sse (a_room_id, a_seq, a_msg_ptr)
		end

	dispatch_broker_replay_sse (a_broker: separate VR_BROKER; a_worker: separate VR_WORKER; a_fd, a_room_id: INTEGER; a_last_seq: NATURAL_64)
			-- Asynchronous command requesting replay from separate broker.
		do
			a_broker.replay_sse_for_worker (a_worker, a_fd, a_room_id, a_last_seq)
		end

	dispatch_broker_update_ws_room (a_broker: separate VR_BROKER; a_room_id, a_worker_id: INTEGER; a_has: BOOLEAN)
			-- Asynchronous command to separate broker.
		do
			a_broker.update_ws_room_worker (a_room_id, a_worker_id, a_has)
		end

	dispatch_broker_update_sse_room (a_broker: separate VR_BROKER; a_room_id, a_worker_id: INTEGER; a_has: BOOLEAN)
			-- Asynchronous command to separate broker.
		do
			a_broker.update_sse_room_worker (a_room_id, a_worker_id, a_has)
		end

feature {NONE} -- Implementation

	connections: HASH_TABLE [VR_CONNECTION, INTEGER]
			-- Worker-local connection directory (strictly non-separate)

	scratch_buffer: MANAGED_POINTER
			-- Dedicated worker scratch memory for non-blocking reads

	expired_fds: ARRAYED_LIST [INTEGER]
			-- Pre-allocated buffer for idle sweep (zero GC allocation)

	sse_heartbeat_buffer: MANAGED_POINTER
			-- Pre-encoded native buffer containing Sse_heartbeat_comment

	close_connection (a_fd: INTEGER)
			-- Remove from tracking, deregister from epoll, and close file descriptor.
		require
			valid_fd: a_fd >= 0
		local
			res: INTEGER
		do
			if attached connections.item (a_fd) as conn then
				-- Remove from all worker-local rooms immediately (Zero ghost deliveries!)
				across conn.rooms as r_ic loop
					if attached rooms.item (r_ic) as set then
						set.remove (a_fd)
						if set.is_empty then
							rooms.remove (r_ic)
							dispatch_broker_update_ws_room (broker, r_ic, worker_id, False)
						end
					end
					if attached sse_rooms.item (r_ic) as sse_set then
						sse_set.remove (a_fd)
						if sse_set.is_empty then
							sse_rooms.remove (r_ic)
							dispatch_broker_update_sse_room (broker, r_ic, worker_id, False)
						end
					end
				end
				sse_active_fds.remove (a_fd)
				conn.mark_closed
				connections.remove (a_fd)
				{VR_REACTOR_BRIDGE}.c_reactor_release_worker_conn (reactor_handle, worker_id)
				if reactor_handle /= default_pointer then
					res := {VR_REACTOR_BRIDGE}.c_reactor_close_fd_for_worker (reactor_handle, a_fd, worker_id)
				else
					res := {VR_REACTOR_BRIDGE}.c_close (a_fd)
				end
				{VR_REACTOR_BRIDGE}.c_metric_dec_open (reactor_handle)
			end
		ensure
			not_in_table: not connections.has (a_fd)
		end

feature -- Testing & Fault Injection

	inject_next_event_fault
			-- Instruct worker to raise an assertion during next socket event turn to test isolated rescue.
		do
			is_fault_injected := True
		ensure
			fault_armed: is_fault_injected
		end

feature {NONE} -- Testing State

	is_fault_injected: BOOLEAN

invariant
	valid_id: worker_id >= 0
	valid_handle: reactor_handle /= default_pointer

end
