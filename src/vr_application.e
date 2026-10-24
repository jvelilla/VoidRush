note
	description: "VoidRush server root class"

class
	VR_APPLICATION

inherit
	VR_REACTOR_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Run VoidRush server.
		local
			engine: VR_ENGINE
		do
			io.put_string ("VoidRush networking engine starting on port " + Default_port.out + " with " + Default_worker_count.out + " workers...%N")
			create engine.make (Default_port, Default_worker_count)
			io.put_string ("VR_ENGINE initialized successfully. Entering event loop...%N")
			engine.start
		end

feature {NONE} -- Constants

	Default_port: INTEGER = 8080
			-- Default server listening port

end
