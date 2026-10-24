note
	description: "Worker processor managing a partition of connections, non-blocking I/O turn, and idle sweeps"

class
	VR_WORKER

inherit
	VR_REACTOR_CONSTANTS

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
		do
			worker_id := a_id
			reactor_handle := a_reactor_handle
			broker := a_broker
			total_workers := a_total_workers
			create connections.make (1024)
			create rooms.make (64)
			create scratch_buffer.make (Scratch_buffer_size)
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

	reactor_handle: POINTER
			-- Pointer to native vr_reactor_t instance

	broker: separate VR_BROKER
			-- Reference to shared separate Pub/Sub broker

	total_workers: INTEGER
			-- Total number of worker shards in reactor pool

	rooms: HASH_TABLE [ARRAYED_LIST [INTEGER], INTEGER]
			-- Worker-local subscription directory: room_id -> list of local client FDs

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
			connections.force (conn, a_fd)
			{VR_REACTOR_BRIDGE}.c_metric_inc_open (reactor_handle)

			-- Worker-exclusive registration to epoll
			res := {VR_REACTOR_BRIDGE}.c_reactor_add (reactor_handle, a_fd, Reactor_in, a_generation)
			if res /= 0 then
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
			read_turn, write_turn, chunk, n, res: INTEGER
			read_done, write_done: BOOLEAN
		do
			if attached connections.item (a_fd) as conn and then conn.matches (a_generation) then
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
							chunk := (Read_budget_per_turn - read_turn).min (scratch_buffer.count)
							n := {VR_REACTOR_BRIDGE}.c_reactor_read (a_fd, scratch_buffer.item, chunk)
							if n > 0 then
								read_turn := read_turn + n
								{VR_REACTOR_BRIDGE}.c_metric_add_bytes_in (reactor_handle, n.to_natural_64)
								conn.on_data_received (scratch_buffer.item, n, now_ms)
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
							if conn.interest_flags /= 0 then
								res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_fd, conn.interest_flags, a_generation)
								if res /= 0 then
									close_connection (a_fd)
								end
							end
						end
					end
				end
			else
				-- Stale event for an obsolete generation or unregistered socket
				{VR_REACTOR_BRIDGE}.c_metric_inc_stale (reactor_handle)
			end
		end

	on_timer_tick (a_now_ms: NATURAL_64)
			-- Worker-local idle and Slowloris connection sweep.
		require
			valid_time: a_now_ms > 0
		local
			expired_fds: ARRAYED_LIST [INTEGER]
		do
			create expired_fds.make (64)
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
			list: ARRAYED_LIST [INTEGER]
		do
			if not rooms.has (a_room_id) then
				create list.make (64)
				rooms.put (list, a_room_id)
			end
			if attached rooms.item (a_room_id) as l and then not l.has (a_fd) then
				l.extend (a_fd)
				if attached connections.item (a_fd) as conn then
					conn.register_room (a_room_id)
				end
			end
		end

	leave_room (a_fd, a_room_id: INTEGER)
			-- Unsubscribe connection `a_fd` from `a_room_id` locally.
		require
			valid_fd: a_fd >= 0
			valid_room: a_room_id >= 0
		do
			if attached rooms.item (a_room_id) as l then
				l.prune_all (a_fd)
			end
			if attached connections.item (a_fd) as conn then
				conn.unregister_room (a_room_id)
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
			total_len, i, target_fd: INTEGER
		do
			if attached rooms.item (a_room_id) as subscriber_list then
				data_ptr := {VR_REACTOR_BRIDGE}.c_ws_msg_data (a_msg_handle)
				total_len := {VR_REACTOR_BRIDGE}.c_ws_msg_size (a_msg_handle)

				from
					i := 1
				until
					i > subscriber_list.count
				loop
					target_fd := subscriber_list.i_th (i)
					if attached connections.item (target_fd) as conn and then conn.proto_state = Proto_ws_open then
						deliver_frame_to_connection (conn, data_ptr, total_len)
					end
					i := i + 1
				end
			end
			-- Decrement reference count; last worker automatically frees C memory
			{VR_REACTOR_BRIDGE}.c_ws_msg_unref (a_msg_handle)
		end

	deliver_frame_to_connection (a_conn: VR_CONNECTION; a_ptr: POINTER; a_len: INTEGER)
			-- Deliver frame using direct non-blocking write fast-path.
		require
			valid_conn: a_conn /= Void
			valid_ptr: a_ptr /= default_pointer
			valid_len: a_len > 0
		local
			n, res: INTEGER
			now_ms: NATURAL_64
		do
			if a_conn.paused_in then
				{VR_REACTOR_BRIDGE}.c_metric_inc_ws_dropped (reactor_handle)
			elseif a_conn.out_buf_bytes = 0 then
				-- Fast Path: Socket idle, write directly to kernel
				n := {VR_REACTOR_BRIDGE}.c_reactor_write (a_conn.fd, a_ptr, a_len)
				now_ms := {VR_REACTOR_BRIDGE}.c_reactor_now_ms
				if n = a_len then
					a_conn.touch (now_ms)
					{VR_REACTOR_BRIDGE}.c_metric_add_bytes_out (reactor_handle, n.to_natural_64)
					{VR_REACTOR_BRIDGE}.c_metric_inc_ws_frames_out (reactor_handle)
				elseif n > 0 then
					a_conn.append_outbound (a_ptr + n, a_len - n)
					a_conn.touch (now_ms)
					{VR_REACTOR_BRIDGE}.c_metric_add_bytes_out (reactor_handle, n.to_natural_64)
					res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_conn.fd, a_conn.interest_flags, a_conn.generation)
				elseif n = Reactor_io_would_block then
					a_conn.append_outbound (a_ptr, a_len)
					res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_conn.fd, a_conn.interest_flags, a_conn.generation)
				else -- Reactor_io_error (-2)
					close_connection (a_conn.fd)
				end
			else
				a_conn.append_outbound (a_ptr, a_len)
				if a_conn.out_buf_bytes >= Out_buf_high_water then
					a_conn.set_paused_in (True)
				end
				res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_conn.fd, a_conn.interest_flags, a_conn.generation)
			end
		end

feature {NONE} -- SCOOP Broker Dispatch

	dispatch_broker_publish (a_broker: separate VR_BROKER; a_room_id: INTEGER; a_msg_ptr: POINTER)
			-- Asynchronous command to controlled separate broker.
		do
			a_broker.publish (a_room_id, a_msg_ptr)
		end

feature {NONE} -- Implementation

	connections: HASH_TABLE [VR_CONNECTION, INTEGER]
			-- Worker-local connection directory (strictly non-separate)

	scratch_buffer: MANAGED_POINTER
			-- Dedicated worker scratch memory for non-blocking reads

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
					if attached rooms.item (r_ic) as l then
						l.prune_all (a_fd)
					end
				end
				conn.mark_closed
				connections.remove (a_fd)
				res := {VR_REACTOR_BRIDGE}.c_reactor_del (reactor_handle, a_fd)
				res := {VR_REACTOR_BRIDGE}.c_close (a_fd)
				{VR_REACTOR_BRIDGE}.c_metric_dec_open (reactor_handle)
			end
		ensure
			not_in_table: not connections.has (a_fd)
		end

invariant
	valid_id: worker_id >= 0
	valid_handle: reactor_handle /= default_pointer

end
