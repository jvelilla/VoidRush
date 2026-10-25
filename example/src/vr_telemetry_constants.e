note
	description: "Constants and configuration defaults for the Telemetry Gateway example"

class
	VR_TELEMETRY_CONSTANTS

inherit
	VR_REACTOR_CONSTANTS
	VR_SSE_CONSTANTS

feature -- Configuration Defaults

	Default_telemetry_port: INTEGER = 4343
			-- Default TCP listening port for the example gateway

	Default_telemetry_workers: INTEGER = 4
			-- Default number of supervised worker partitions

	Default_telemetry_room: INTEGER = 42
			-- Default room ID used for sensor fan-out and spectator streaming

feature -- Banner & Presentation

	Telemetry_banner: STRING_8 = "[
======================================================================
  VoidRush Telemetry Gateway & Chaos Resilience Example
  High-Frequency Event Fan-Out + OTP-Inspired Supervisor Plane
======================================================================
  Listening on port:       4343
  Supervised Workers:      4 (SCOOP Regions)
  Native Reactor:          C Edge-Triggered epoll (Dual-Buffer Zero-Alloc)
  Resilience Model:        OTP-Style Sliding Window (3 restarts / 30s)
                           Lock-Free C Atomic Liveness Monitoring

  Endpoints:
    GET  /healthz          Fast-path slice health probe (zero allocation)
    GET  /metrics          Live atomic Prometheus metrics export
    GET  /events?room=42   WHATWG SSE live event stream (with Last-Event-ID)
    GET  /ws               RFC 6455 WebSocket bidirectional telemetry channel
    POST /events           HTTP/1.1 keep-alive high-rate ingest fan-out
    POST /debug/fault      Controlled chaos injection (?type=conn|worker|broker)
======================================================================
]"

end
