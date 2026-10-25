note
	description: "Sliding-window restart rate limiter enforcing OTP-style intensity boundaries"

class
	VR_RESTART_INTENSITY

create
	make,
	make_custom

feature {NONE} -- Initialization

	make
			-- Initialize with default OTP parameters: 3 restarts in 30 seconds.
		do
			make_custom (3, 30000)
		ensure
			default_max_restarts: max_restarts = 3
			default_window: window_ms = 30000
		end

	make_custom (a_max_restarts: INTEGER; a_window_ms: NATURAL_64)
			-- Initialize with custom intensity boundaries.
		require
			valid_max: a_max_restarts > 0
			valid_window: a_window_ms > 0
		do
			max_restarts := a_max_restarts
			window_ms := a_window_ms
			create timestamps.make (a_max_restarts)
		ensure
			max_restarts_set: max_restarts = a_max_restarts
			window_set: window_ms = a_window_ms
			empty_timestamps: timestamps.is_empty
		end

feature -- Access

	max_restarts: INTEGER
			-- Maximum restarts permitted within window_ms

	window_ms: NATURAL_64
			-- Sliding time window in milliseconds

	count: INTEGER
			-- Number of restarts recorded in current window
		do
			Result := timestamps.count
		end

feature -- Status Report

	allow_restart (a_now_ms: NATURAL_64): BOOLEAN
			-- Does the sliding window permit a restart at timestamp `a_now_ms`?
		require
			valid_timestamp: a_now_ms > 0
		do
			purge_expired (a_now_ms)
			Result := timestamps.count < max_restarts
		ensure
			within_limit: Result = (timestamps.count < max_restarts)
		end

feature -- Modification

	record_restart (a_now_ms: NATURAL_64)
			-- Record a restart event at timestamp `a_now_ms`.
		require
			valid_timestamp: a_now_ms > 0
			permitted: allow_restart (a_now_ms)
		do
			purge_expired (a_now_ms)
			timestamps.extend (a_now_ms)
		ensure
			count_incremented: timestamps.count = old count + 1
		end

	reset
			-- Clear all recorded restart timestamps.
		do
			timestamps.wipe_out
		ensure
			is_empty: timestamps.is_empty
		end

feature {NONE} -- Implementation

	timestamps: ARRAYED_LIST [NATURAL_64]
			-- Sorted timestamps of recent restarts within current window

	purge_expired (a_now_ms: NATURAL_64)
			-- Remove timestamps older than `a_now_ms - window_ms`.
		local
			cutoff: NATURAL_64
		do
			if a_now_ms > window_ms then
				cutoff := a_now_ms - window_ms
			else
				cutoff := 0
			end
			from
			until
				timestamps.is_empty or else timestamps.first >= cutoff
			loop
				timestamps.start
				timestamps.remove
			end
		ensure
			purged: timestamps.is_empty or else timestamps.first >= (if a_now_ms > window_ms then a_now_ms - window_ms else {NATURAL_64} 0 end)
		end

invariant
	valid_max: max_restarts > 0
	valid_window: window_ms > 0
	count_within_bounds: count >= 0 and count <= max_restarts

end
