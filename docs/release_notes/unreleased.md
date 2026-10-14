# Unreleased

## Protocol baseline

- Device Gateway: `b0bbeab8fef29f7607f432f0cfa481409410dca8` (`main`).
- Agent Gateway: `2986083b5c2e570ed3df0242c97532e556b62a70` (`master`).
- Application-side protocol checked against LobeHub `6c7a6fb033e443bc27bf34cbd5509ab3db7f22f3`: `packages/device-gateway-client`, `packages/agent-gateway-client`, `apps/server/src/modules/AgentRuntime/GatewayStreamNotifier.ts`, and `apps/server/src/router-hono/agent/handlers`.

## Device Gateway

- Authenticated `device_metrics` uploads receive `device_metrics_ack`; `POST /api/device/metrics` returns ascending, user/device-scoped health samples and supports inclusive `since` queries while devices are offline.
- Device health history is held in memory for up to 48 hours, with timestamp deduplication, 500-entry batch processing, sample validation, and 10-minute future clock tolerance. Fixed limits are 3,000 samples per device, 300,000 samples globally, and 1,000 device histories. Global pressure evicts the history with the oldest latest sample; expired histories are cleaned every minute without requiring device traffic.
- Gateway restart loses health history. New uploads and unacknowledged device backlog repopulate it; no disk persistence or external runtime dependency is required. Gateway management metrics remain excluded.
- Tool calls preserve their request ID across reconnects, bind to the initially selected physical device and connection, and allow three retransmissions. The execution timeout has a 45-second recovery window. Calls with no successful delivery return `503 DEVICE_OFFLINE`; delivered calls without a response return `504 TIMEOUT`.

## Agent Gateway

- `/api/operations/llm-execute`, `llm-cancel` and `llm-close` deliver and retire relayed LLM attempts. Execute responses include the number of recipients and reject terminal operations with `409 OPERATION_ENDED`.
- Execute events prefer the starter's `clientId`; v2 falls back to executor subscriptions and then all subscribers. Cancellation reaches every subscriber. Live delivery and replay use the same recipient rules.
- Only open, unexpired attempts replay. Open attempts remain available after event-buffer trimming, and stop replaying after close, cancellation or terminal operation status. They remain in memory and are lost on restart.
- Initialization arms the inactivity watchdog. All backend activity refreshes a fixed 30-minute window, or 60 minutes for runs with nonempty `init.meta.heteroType`.

## Deployment and scope

- Device routing requires `userId`. Workspace device pools and workspace JWT routing are excluded from the open-source self-hosted target. Extra `workspaceId` and `ingestWorkspaceId` fields remain opaque forwarded fields in agent-run requests and do not select a device pool.
- Tool-call recovery can exceed the generic HTTP write timeout; reverse proxies must allow the execution timeout plus the 45-second recovery window.
- Admin APIs (including `/api/admin/tunnels*`), device tunnels (the LobeHub alpha experiment, including path-based HTTP/WebSocket forwarding, the slug registry, hostname ingress and session cookies), admin connection/error reporting, geo reporting, Durable Object persistence/hibernation and multi-instance coordination remain excluded.
- No external runtime services or dependencies are required. Operations, open LLM attempts, subscriptions, pending requests remain single-process in-memory state.
