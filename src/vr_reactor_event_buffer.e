note
	description: "Pre-allocated, GC-stable memory buffer for receiving native vr_reactor_event_t records"

class
	VR_REACTOR_EVENT_BUFFER

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make (a_capacity: INTEGER)
			-- Initialize buffer capable of holding `a_capacity` event records.
		require
			positive_capacity: a_capacity > 0
		do
			capacity := a_capacity
			create buffer.make (a_capacity * Event_size)
		ensure
			capacity_set: capacity = a_capacity
		end

feature -- Access

	capacity: INTEGER
			-- Maximum number of events buffer can store

	base_address: POINTER
			-- Direct pointer to unmanaged buffer memory
		do
			Result := buffer.item
		ensure
			valid_pointer: Result /= default_pointer
		end

	item_fd (i: INTEGER): INTEGER
			-- File descriptor stored in event record at index `i` (0-indexed)
		require
			valid_index: i >= 0 and i < capacity
		do
			Result := buffer.read_integer_32 (i * Event_size)
		end

	item_generation (i: INTEGER): NATURAL_32
			-- Generation token stored in event record at index `i` (0-indexed)
		require
			valid_index: i >= 0 and i < capacity
		do
			Result := buffer.read_natural_32 (i * Event_size + 4)
		end

	item_flags (i: INTEGER): NATURAL_32
			-- Event flags bitmask stored in event record at index `i` (0-indexed)
		require
			valid_index: i >= 0 and i < capacity
		do
			Result := buffer.read_natural_32 (i * Event_size + 8)
		end

feature {NONE} -- Implementation

	buffer: MANAGED_POINTER
			-- Unmanaged memory buffer backing event records

invariant
	positive_capacity: capacity > 0

end
