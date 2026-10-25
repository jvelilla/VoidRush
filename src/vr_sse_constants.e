note
	description: "Symbolic constants and defaults for WHATWG Server-Sent Events"

class
	VR_SSE_CONSTANTS

feature -- Timing & Limits

	Sse_heartbeat_interval_ms: NATURAL_64 = 15000
			-- 15 seconds between comment heartbeats

	Sse_default_retry_ms: INTEGER = 3000
			-- 3000 ms reconnection retry delay for clients

	Sse_max_payload_size: INTEGER = 65536
			-- 64 KiB maximum allowed event payload

feature -- Pre-Framed Static Headers & Heartbeats

	Sse_handshake_headers: STRING_8 = "HTTP/1.1 200 OK%R%NContent-Type: text/event-stream%R%NCache-Control: no-cache%R%NConnection: keep-alive%R%NX-Accel-Buffering: no%R%N%R%Nretry: 3000%N%N"
			-- HTTP/1.1 200 OK headers with retry directive

	Sse_heartbeat_comment: STRING_8 = ": heartbeat%N%N"
			-- 13-byte lightweight comment heartbeat

	Sse_accepted_post_response: STRING_8 = "HTTP/1.1 202 Accepted%R%NContent-Length: 0%R%NConnection: keep-alive%R%N%R%N"
			-- Fast acknowledgment for POST /events publish endpoint

end
