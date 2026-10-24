note
	description: "Worker processor managing a partition of connections and event handling"

class
	VR_WORKER

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_id: INTEGER; a_reactor_handle: POINTER)
			-- Initialize worker `a_id` linked to native reactor `a_reactor_handle`.
		require
			valid_id: a_id >= 0
			valid_handle: a_reactor_handle /= default_pointer
		do
			worker_id := a_id
			reactor_handle := a_reactor_handle
			create connections.make (1024)
			is_shutdown_complete := False
		ensure
			id_set: worker_id = a_id
			handle_set: reactor_handle = a_reactor_handle
			not_shutdown: not is_shutdown_complete
			empty_connections: connections.is_empty
		end

feature -- Access

	worker_id: INTEGER
			-- Unique partition index

	reactor_handle: POINTER
			-- Pointer to native vr_reactor_t instance

	is_shutdown_complete: BOOLEAN
			-- Has worker cleanly closed all connections during shutdown?

	connection_count: INTEGER
			-- Number of currently tracked active connections
		do
			Result := connections.count
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
		do
			if connections.has (a_fd) then
				-- Defensive cleanup if OS recycled FD before old close was processed
				close_connection (a_fd)
			end
			create conn.make (a_fd, a_generation)
			connections.force (conn, a_fd)
			{VR_REACTOR_BRIDGE}.c_metric_inc_open (reactor_handle)

			res := {VR_REACTOR_BRIDGE}.c_reactor_add (reactor_handle, a_fd, Reactor_in, a_generation)
			if res /= 0 then
				close_connection (a_fd)
			end
		ensure
			connection_tracked_or_cleaned: connections.has (a_fd) implies (attached connections.item (a_fd) as c and then c.generation = a_generation)
		end

	on_socket_event (a_fd: INTEGER; a_generation, a_flags: NATURAL_32)
			-- Process readiness notification dispatched from reactor.
		require
			valid_fd: a_fd >= 0
		local
			res: INTEGER
		do
			if attached connections.item (a_fd) as conn and then conn.matches (a_generation) then
				if (a_flags & (Reactor_err | Reactor_hup)) /= 0 then
					close_connection (a_fd)
				elseif (a_flags & Reactor_in) /= 0 then
					res := {VR_REACTOR_BRIDGE}.c_reactor_drain_discard (a_fd)
					if res = Reactor_io_would_block then
						-- Socket drained cleanly; re-arm EPOLLONESHOT for next event
						res := {VR_REACTOR_BRIDGE}.c_reactor_mod (reactor_handle, a_fd, Reactor_in, a_generation)
						if res /= 0 then
							close_connection (a_fd)
						end
					else
						-- EOF (0) or Fatal Error (-2): peer closed or reset connection
						close_connection (a_fd)
					end
				end
			else
				-- Stale event for an obsolete generation or unregistered socket
				{VR_REACTOR_BRIDGE}.c_metric_inc_stale (reactor_handle)
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

feature {NONE} -- Implementation

	connections: HASH_TABLE [VR_CONNECTION, INTEGER]
			-- Worker-local connection directory (strictly non-separate)

	close_connection (a_fd: INTEGER)
			-- Remove from tracking, deregister from epoll, and close file descriptor.
		require
			valid_fd: a_fd >= 0
		local
			res: INTEGER
		do
			if attached connections.item (a_fd) as conn then
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
