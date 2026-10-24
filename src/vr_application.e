note
	description: "VoidRush server root class"

class
	VR_APPLICATION

create
	make

feature {NONE} -- Initialization

	make
			-- Run VoidRush server.
		local
			reactor: separate VR_REACTOR
		do
			io.put_string ("VoidRush networking engine starting on port " + Default_port.out + "...%N")
			create reactor.make (Default_port)
			io.put_string ("VR_REACTOR initialized successfully. Entering event loop...%N")
			run_reactor (reactor)
		end

feature {NONE} -- Implementation

	run_reactor (a_reactor: separate VR_REACTOR)
			-- Run event loop on `a_reactor`.
		local
			running: BOOLEAN
		do
			from
				running := True
			until
				not running
			loop
				a_reactor.step (1000)
			end
		end

feature {NONE} -- Constants

	Default_port: INTEGER = 8080
			-- Default server listening port

end
