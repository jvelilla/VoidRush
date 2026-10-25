note
	description: "Specification and state tracking for a supervised VoidRush component"

class
	VR_CHILD_SPEC

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_id: STRING_8; a_kind: INTEGER; a_worker_index: INTEGER; a_restart_policy: INTEGER; a_shutdown_timeout_ms: INTEGER)
			-- Initialize child specification.
		require
			valid_id: not a_id.is_empty
			valid_kind: a_kind = Child_reactor or a_kind = Child_broker or a_kind = Child_worker
			valid_worker_index: a_kind = Child_worker implies (a_worker_index >= 0 and a_worker_index < Max_workers)
			valid_policy: a_restart_policy >= Restart_permanent and a_restart_policy <= Restart_temporary
			valid_timeout: a_shutdown_timeout_ms > 0
		do
			id := a_id
			kind := a_kind
			worker_index := a_worker_index
			restart_policy := a_restart_policy
			shutdown_timeout_ms := a_shutdown_timeout_ms
			state := State_stopped
			last_restart_count := 0
		ensure
			id_set: id.same_string (a_id)
			kind_set: kind = a_kind
			worker_index_set: worker_index = a_worker_index
			policy_set: restart_policy = a_restart_policy
			timeout_set: shutdown_timeout_ms = a_shutdown_timeout_ms
			initial_state: state = State_stopped
			zero_restarts: last_restart_count = 0
		end

feature -- Access

	id: STRING_8
			-- Unique identifier string for logging and telemetry

	kind: INTEGER
			-- Child component type (Child_reactor, Child_broker, Child_worker)

	worker_index: INTEGER
			-- Partition slot index (0..N-1 for workers, -1 for reactor/broker)

	restart_policy: INTEGER
			-- Restart policy (Restart_permanent, Restart_transient, Restart_temporary)

	shutdown_timeout_ms: INTEGER
			-- Grace period allowed for child shutdown before forced termination

	state: INTEGER
			-- Current lifecycle state (State_stopped..State_failed)

	last_restart_count: INTEGER
			-- Cumulative count of restarts recorded for this child

feature -- State Modification

	set_state (a_state: INTEGER)
			-- Transition child lifecycle state.
		require
			valid_state: a_state = State_stopped or a_state = State_starting or a_state = State_running or a_state = State_stopping or a_state = State_failed
		do
			state := a_state
		ensure
			state_set: state = a_state
		end

	record_restart
			-- Increment cumulative restart counter.
		do
			last_restart_count := last_restart_count + 1
		ensure
			restarts_incremented: last_restart_count = old last_restart_count + 1
		end

invariant
	id_not_empty: not id.is_empty
	valid_kind: kind = Child_reactor or kind = Child_broker or kind = Child_worker
	valid_state: state = State_stopped or state = State_starting or state = State_running or state = State_stopping or state = State_failed
	valid_restarts: last_restart_count >= 0

end
