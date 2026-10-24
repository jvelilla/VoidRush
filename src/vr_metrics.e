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
			refresh (a_handle)
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

feature -- Status Report

	refresh (a_handle: POINTER)
			-- Refresh metric fields from native reactor structure.
		require
			valid_handle: a_handle /= default_pointer
		local
			buf: MANAGED_POINTER
		do
			create buf.make (Metrics_size)
			{VR_REACTOR_BRIDGE}.c_reactor_get_metrics (a_handle, buf.item)
			active_connections := buf.read_natural_64 (0)
			accepts_total := buf.read_natural_64 (8)
			events_total := buf.read_natural_64 (16)
			stale_events := buf.read_natural_64 (24)
		end

feature -- Output

	out: STRING
			-- Human-readable metrics snapshot.
		do
			create Result.make (128)
			Result.append ("Metrics [accepts=" + accepts_total.out)
			Result.append (", active=" + active_connections.out)
			Result.append (", events=" + events_total.out)
			Result.append (", stale=" + stale_events.out + "]")
		end

end
