note
	description: "RFC 6455 WebSocket constants, opcodes, close codes, and protocol GUID"

class
	VR_WS_CONSTANTS

inherit
	VR_REACTOR_CONSTANTS

feature -- WebSocket GUID

	Ws_guid: STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
			-- Magic string defined by RFC 6455 §1.3

	Ws_guid_length: INTEGER = 36
			-- Exact character count of Ws_guid

end
