note
	description: "Non-separate TCP connection state owned strictly by a single VR_WORKER"

class
	VR_CONNECTION

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_fd: INTEGER; a_generation: NATURAL_32)
			-- Initialize connection for socket `a_fd` protected by `a_generation`.
		require
			valid_fd: a_fd >= 0
			valid_generation: a_generation > 0
		do
			fd := a_fd
			generation := a_generation
			state := State_open
		ensure
			fd_set: fd = a_fd
			generation_set: generation = a_generation
			is_open: is_open
		end

feature -- Access

	fd: INTEGER
			-- Socket file descriptor

	generation: NATURAL_32
			-- Monotonic generation token assigned at accept

	state: INTEGER
			-- Lifecycle state (State_open, State_closing, State_closed)

feature -- Status Report

	matches (a_generation: NATURAL_32): BOOLEAN
			-- Does incoming event generation match active connection?
		do
			Result := (generation = a_generation) and is_open
		end

	is_open: BOOLEAN
			-- Is connection active?
		do
			Result := state = State_open
		end

	is_closed: BOOLEAN
			-- Has connection transitioned to closed?
		do
			Result := state = State_closed
		end

feature -- State Transitions

	mark_closing
			-- Mark connection as closing down.
		require
			is_open: is_open
		do
			state := State_closing
		ensure
			is_closing: state = State_closing
		end

	mark_closed
			-- Finalize connection closure.
		require
			not_closed: not is_closed
		do
			state := State_closed
		ensure
			is_closed: is_closed
		end

invariant
	valid_fd: fd >= 0
	valid_generation: generation > 0
	valid_state: state = State_open or state = State_closing or state = State_closed

end
