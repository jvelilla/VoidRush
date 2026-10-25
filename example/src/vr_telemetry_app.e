note
	description: "Entry point for the VoidRush Telemetry Gateway example"

class
	VR_TELEMETRY_APP

inherit
	VR_TELEMETRY_CONSTANTS

create
	make

feature {NONE} -- Initialization

	make
			-- Bootstrap and run VoidRush Telemetry Gateway.
		local
			engine: VR_ENGINE
		do
			io.put_string (Telemetry_banner)
			io.put_new_line
			io.put_string ("[telemetry_app] Initializing VR_ENGINE on port " + Default_telemetry_port.out + " with " + Default_telemetry_workers.out + " workers...%N")

			create engine.make (Default_telemetry_port, Default_telemetry_workers)

			io.put_string ("[telemetry_app] VR_ENGINE online. Root supervisor active. Entering event loop...%N")
			engine.start
			engine.run
			io.put_string ("[telemetry_app] VoidRush Telemetry Gateway terminated cleanly.%N")
		end

end
