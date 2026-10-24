note
	description: "Automated test harness for VoidRush Eiffel VR_REACTOR"

class
	VR_TEST_APP

create
	make

feature {NONE} -- Initialization

	make
			-- Run automated tests on Eiffel reactor and event buffer.
		do
			io.put_string ("[vr_test_app] Testing VR_REACTOR_EVENT_BUFFER...%N")
			test_event_buffer

			io.put_string ("[vr_test_app] Testing VR_REACTOR initialization and shutdown...%N")
			test_reactor_lifecycle

			io.put_string ("[vr_test_app] All Eiffel tests passed successfully!%N")
		end

feature {NONE} -- Tests

	test_event_buffer
			-- Verify VR_REACTOR_EVENT_BUFFER indexing and capacity.
		local
			buf: VR_REACTOR_EVENT_BUFFER
		do
			create buf.make (64)
			check capacity_ok: buf.capacity = 64 end
			check pointer_valid: buf.base_address /= default_pointer end
			check initial_fd: buf.item_fd (0) = 0 end
			check initial_gen: buf.item_generation (0) = 0 end
			check initial_flags: buf.item_flags (0) = 0 end
		end

	test_reactor_lifecycle
			-- Verify VR_REACTOR initialization, step, and clean shutdown under SCOOP.
		local
			r: separate VR_REACTOR
		do
			create r.make (Test_port)
			run_reactor_steps (r)
		end

	run_reactor_steps (r: separate VR_REACTOR)
			-- Run 2 non-blocking poll steps and shut down.
		do
			r.step (50)
			r.step (50)
			r.shutdown
		end

feature {NONE} -- Constants

	Test_port: INTEGER = 19877
			-- Dedicated test port

end
