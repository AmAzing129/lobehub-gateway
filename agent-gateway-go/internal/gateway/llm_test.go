package gateway

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestLLMV1TargetsStarterAndCancelsEveryClient(t *testing.T) {
	s := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "llm", "userId": "u1"}, 200)
	connect := func(id string) *testWS {
		ws := dialWebSocket(t, ts.URL, "/ws?operationId=llm")
		ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token", "clientId": id})
		if ws.readJSON(t)["type"] != "auth_success" {
			t.Fatal("auth failed")
		}
		return ws
	}
	starter, viewer := connect("starter"), connect("viewer")
	defer starter.close()
	defer viewer.close()
	body := map[string]any{"operationId": "llm", "data": map[string]any{"callId": "call-1", "preferredClientId": "starter", "opaque": map[string]any{"model": "local"}}}
	result := postJSONBody(t, ts.URL+"/api/operations/llm-execute", body, 200)
	if result["delivered"] != float64(1) {
		t.Fatalf("unexpected delivered: %v", result)
	}
	event := starter.readJSON(t)["event"].(map[string]any)
	if event["type"] != "llm_execute" || event["data"].(map[string]any)["opaque"] == nil {
		t.Fatalf("unexpected execute: %v", event)
	}
	wsExpectNoMessage(t, viewer, 20*time.Millisecond)
	viewer.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "0", "wantStatus": true})
	if done := viewer.readJSON(t); done["type"] != "resume_complete" {
		t.Fatalf("viewer replayed starter's call: %v", done)
	}
	postJSON(t, ts.URL+"/api/operations/llm-cancel", body, 200)
	for _, ws := range []*testWS{starter, viewer} {
		if event := ws.readJSON(t)["event"].(map[string]any); event["type"] != "llm_cancel" {
			t.Fatalf("unexpected cancel: %v", event)
		}
	}
	op := s.getOperation("llm")
	events, _, _ := op.collectReplay("0")
	for _, payload := range events {
		if llmPayload(payload).Type == "llm_execute" {
			t.Fatal("cancelled attempt replayed")
		}
	}
}

func TestLLMV2RoutingAndReplay(t *testing.T) {
	_, ts, sign := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "llm", "userId": "u1"}, 200)
	token := url.QueryEscape(sign("u1", time.Now().Add(time.Minute), time.Now().Add(-time.Minute)))
	connect := func(id string, executor bool) *testWS {
		ws := dialWebSocket(t, ts.URL, "/v2/ws?token="+token+"&clientId="+id)
		_ = ws.readJSON(t)
		ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "llm", "executor": executor})
		readUntil(t, ws, func(m map[string]any) bool { return m["type"] == "resume_complete" })
		return ws
	}
	starter, executor, viewer := connect("starter", false), connect("executor", true), connect("viewer", false)
	defer starter.close()
	defer executor.close()
	defer viewer.close()
	execute := func(id, preferred string) map[string]any {
		return postJSONBody(t, ts.URL+"/api/operations/llm-execute", map[string]any{"operationId": "llm", "data": map[string]any{"callId": id, "preferredClientId": preferred}}, 200)
	}
	if result := execute("call-1", "starter"); result["delivered"] != float64(1) {
		t.Fatalf("unexpected delivery: %v", result)
	}
	if m := starter.readJSON(t); llmPayload(mustMarshal(m)).Type != "llm_execute" {
		t.Fatalf("missing execute: %v", m)
	}
	wsExpectNoMessage(t, executor, 20*time.Millisecond)
	wsExpectNoMessage(t, viewer, 20*time.Millisecond)
	// Re-subscription must use the same routing choice as live delivery.
	viewer.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "llm", "lastEventId": "0"})
	if m := viewer.readJSON(t); m["type"] != "resume_complete" {
		t.Fatalf("viewer replayed targeted call: %v", m)
	}
	if result := execute("call-2", "absent"); result["delivered"] != float64(1) {
		t.Fatalf("unexpected fallback count: %v", result)
	}
	if m := executor.readJSON(t); llmPayload(mustMarshal(m)).Data.CallID != "call-2" {
		t.Fatalf("executor did not get fallback: %v", m)
	}
	wsExpectNoMessage(t, starter, 20*time.Millisecond)
	wsExpectNoMessage(t, viewer, 20*time.Millisecond)
}

func TestLLMReplayOpenCallsSurviveBufferTrim(t *testing.T) {
	s := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "llm", "userId": "u1"}, 200)
	body := map[string]any{"operationId": "llm", "data": map[string]any{"callId": "open"}}
	if result := postJSONBody(t, ts.URL+"/api/operations/llm-execute", body, 200); result["delivered"] != float64(0) {
		t.Fatal(result)
	}
	op := s.getOperation("llm")
	// Trim the first attempt out of the normal replay ring.
	for i := 0; i < eventBufferMax+1; i++ {
		op.pushEvent(agentStreamEvent{Type: "step_complete", Data: json.RawMessage(`{}`)})
	}
	events, gap, _ := op.collectReplay("0")
	if !gap || llmPayload(events[0]).Data.CallID != "open" {
		t.Fatal("open call lost during trim")
	}
	postJSON(t, ts.URL+"/api/operations/llm-close", map[string]any{"operationId": "llm", "callId": "open"}, 200)
	events, _, _ = op.collectReplay("0")
	for _, payload := range events {
		if llmPayload(payload).Type == "llm_execute" {
			t.Fatal("closed call replayed")
		}
	}
	postJSON(t, ts.URL+"/api/operations/llm-execute", body, 200)
	op.mu.Lock()
	call := op.openLLM["open"]
	call.expiresAt = time.Now().Add(-time.Second)
	op.openLLM["open"] = call
	op.mu.Unlock()
	events, _, _ = op.collectReplay("0")
	for _, payload := range events {
		if llmPayload(payload).Type == "llm_execute" {
			t.Fatal("expired call replayed")
		}
	}
	postJSON(t, ts.URL+"/api/operations/llm-execute", body, 200)
	postJSON(t, ts.URL+"/api/operations/update-status", map[string]any{"operationId": "llm", "status": "completed"}, 200)
	events, _, _ = op.collectReplay("0")
	for _, payload := range events {
		if llmPayload(payload).Type == "llm_execute" {
			t.Fatal("terminal call replayed")
		}
	}
	result := postJSONBody(t, ts.URL+"/api/operations/llm-execute", body, 409)
	if result["error"] != "OPERATION_ENDED" || result["delivered"] != float64(0) {
		t.Fatal(result)
	}
	postJSON(t, ts.URL+"/api/operations/llm-execute", map[string]any{"operationId": "llm", "data": map[string]any{}}, 400)
}
