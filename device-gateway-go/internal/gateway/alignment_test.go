package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDeviceRoutingUsesUserID(t *testing.T) {
	s := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()
	res := postJSON(t, ts.URL+"/api/device/status", "service-token", `{"workspaceId":"w1"}`)
	assertStatus(t, res, http.StatusBadRequest)
	assertBody(t, res, "Missing userId")
	res, err := http.Get(ts.URL + "/ws?workspaceId=w1&deviceId=d1")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, res, http.StatusBadRequest)
	assertBody(t, res, "Missing userId")
	// Extra fields do not redirect a personal device into a separate device pool.
	ws := dialTestWS(t, ts.URL, "/ws?userId=u1&workspaceId=w1&deviceId=d1")
	defer ws.close()
	ws.sendJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	assertWSJSON(t, ws, map[string]any{"type": "auth_success"})
	for _, body := range []string{`{"userId":"u1"}`, `{"userId":"u1","workspaceId":"w2"}`} {
		res = postJSON(t, ts.URL+"/api/device/status", "service-token", body)
		assertStatus(t, res, http.StatusOK)
		assertJSON(t, res, map[string]any{"deviceCount": float64(1), "online": true})
	}
	res = postJSON(t, ts.URL+"/api/device/status", "service-token", `{"userId":"u2","workspaceId":"w1"}`)
	assertJSON(t, res, map[string]any{"deviceCount": float64(0), "online": false})
}

func TestToolCallReconnectKeepsRequestAndConnectionIdentity(t *testing.T) {
	s := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()
	connect := func(path string) *testWS {
		ws := dialTestWS(t, ts.URL, path)
		ws.sendJSON(t, map[string]any{"type": "auth", "token": "service-token"})
		assertWSJSON(t, ws, map[string]any{"type": "auth_success"})
		return ws
	}
	path := "/ws?userId=u1&deviceId=d1&connectionId=cli-1&channel=cli"
	ws := connect(path)
	result := make(chan dispatchStatus, 1)
	h := s.hub("u1")
	payload := map[string]any{"type": "tool_call_request", "requestId": "stable-id", "toolCall": map[string]any{"apiName": "echo"}}
	go func() { _, status := h.dispatchToolCall("d1", "stable-id", 2*time.Second, payload); result <- status }()
	first := ws.readJSON(t)
	ws.close()
	other := connect("/ws?userId=u1&deviceId=d1&connectionId=desktop-1&channel=desktop")
	defer other.close()
	wsExpectNoMessage(t, other, 30*time.Millisecond)
	replacement := connect(path)
	defer replacement.close()
	retried := replacement.readJSON(t)
	if !bytes.Equal(mustJSON(t, first), mustJSON(t, retried)) {
		t.Fatalf("retry changed request: %v / %v", first, retried)
	}
	replacement.sendJSON(t, map[string]any{"type": "tool_call_response", "requestId": "stable-id", "result": map[string]any{"success": true}})
	if <-result != dispatchOK {
		t.Fatal("retry did not resolve original request")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestToolCallWaitsForInitiallyOfflineDevice(t *testing.T) {
	h := newHub("u1")
	_, status := h.dispatchToolCall("d1", "missing", 15*time.Millisecond, map[string]any{"type": "tool_call_request"})
	if status != dispatchOffline {
		t.Fatalf("want offline after recovery, got %v", status)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.pending) != 0 {
		t.Fatal("expired request leaked")
	}
}
