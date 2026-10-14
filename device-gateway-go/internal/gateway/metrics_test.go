package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func metricJSON(at float64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"connected":true,"cpuCount":8,"cpuPercent":25,"load1":null,"load5":1.5,"load15":null,"memoryTotalBytes":16000,"memoryUsedBytes":8000,"observedAt":%v}`, at))
}

func testMetricStore(t *testing.T) *metricStore {
	t.Helper()
	s := newMetricStore()
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.timer != nil {
			s.timer.Stop()
		}
	})
	return s
}

func TestMetricSampleValidation(t *testing.T) {
	var fields map[string]any
	_ = json.Unmarshal(metricJSON(1000), &fields)
	tests := []struct {
		name, field string
		value       any
		missing     bool
	}{
		{name: "missing connected", field: "connected", missing: true},
		{name: "missing nullable", field: "load1", missing: true},
		{name: "null required", field: "cpuCount", value: nil},
		{name: "wrong boolean", field: "connected", value: 1},
		{name: "wrong number", field: "memoryUsedBytes", value: "8000"},
		{name: "negative memory", field: "memoryTotalBytes", value: -1},
		{name: "negative load", field: "load1", value: -1},
		{name: "invalid CPU", field: "cpuPercent", value: 101},
		{name: "negative timestamp", field: "observedAt", value: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copy := make(map[string]any, len(fields))
			for key, value := range fields {
				copy[key] = value
			}
			if test.missing {
				delete(copy, test.field)
			} else {
				copy[test.field] = test.value
			}
			raw, _ := json.Marshal(copy)
			if _, ok := decodeMetricSample(raw); ok {
				t.Fatalf("accepted invalid sample: %s", raw)
			}
		})
	}
	for _, raw := range []json.RawMessage{metricJSON(1000), json.RawMessage(`{"connected":false,"cpuCount":0,"cpuPercent":null,"load1":null,"load5":null,"load15":null,"memoryTotalBytes":0,"memoryUsedBytes":0,"observedAt":0}`)} {
		if _, ok := decodeMetricSample(raw); !ok {
			t.Fatalf("rejected valid sample: %s", raw)
		}
	}
}

func TestMetricRetentionDedupeAndQuery(t *testing.T) {
	s := testMetricStore(t)
	now := time.Now().Truncate(time.Millisecond)
	at := func(delta time.Duration) float64 { return float64(now.Add(delta).UnixMilli()) }
	key := metricKey{"u1", "d1"}
	raw := []json.RawMessage{metricJSON(at(-time.Hour)), metricJSON(at(-metricRetention)), metricJSON(at(-metricRetention - time.Millisecond)), metricJSON(at(metricFutureSkew)), metricJSON(at(metricFutureSkew + time.Millisecond)), json.RawMessage(`{"connected":true}`)}
	if accepted := s.store(key, raw, now); accepted != 3 {
		t.Fatalf("accepted=%d want 3", accepted)
	}
	replacement := metricJSON(at(-time.Hour))
	var fields map[string]any
	_ = json.Unmarshal(replacement, &fields)
	fields["cpuPercent"] = 75
	fields["extra"] = "drop me"
	replacement, _ = json.Marshal(fields)
	if accepted := s.store(key, []json.RawMessage{replacement, replacement}, now); accepted != 2 {
		t.Fatal("duplicates must still count as accepted")
	}
	samples := s.read(key, 0, now)
	if len(samples) != 3 || samples[0].ObservedAt != at(-metricRetention) || *samples[1].CPUPercent != 75 || samples[2].ObservedAt != at(metricFutureSkew) {
		t.Fatalf("unexpected retained history: %+v", samples)
	}
	if samples[1].Load1 != nil || samples[1].Load15 != nil {
		t.Fatal("null readings changed")
	}
	encoded, _ := json.Marshal(samples)
	var results []map[string]any
	_ = json.Unmarshal(encoded, &results)
	if _, ok := results[1]["extra"]; ok {
		t.Fatal("unknown fields stored")
	}
	if got := s.read(key, at(-time.Hour), now); len(got) != 2 {
		t.Fatal("since must be inclusive")
	}
	if got := s.read(key, at(metricFutureSkew+time.Millisecond), now); len(got) != 0 {
		t.Fatal("future query returned samples")
	}
	for _, other := range []metricKey{{"u2", "d1"}, {"u1", "d2"}} {
		if got := s.read(other, 0, now); got == nil || len(got) != 0 {
			t.Fatal("history leaked across identities or returned null")
		}
	}
	// A query snapshot must not share the mutable series backing array.
	samples[1].ObservedAt = 0
	if got := s.read(key, 0, now); got[1].ObservedAt != at(-time.Hour) {
		t.Fatal("query mutated stored series")
	}
	_ = s.read(key, 0, now.Add(metricRetention+metricFutureSkew+time.Millisecond))
	if s.total != 0 || len(s.series) != 0 {
		t.Fatal("expired device history retained")
	}
}

func TestMetricBatchAndDeviceCapacity(t *testing.T) {
	s := testMetricStore(t)
	now := time.Now().Truncate(time.Millisecond)
	base := float64(now.Add(-time.Hour).UnixMilli())
	key := metricKey{"u1", "d1"}
	raw := make([]json.RawMessage, metricMaxBatch+1)
	for i := range raw {
		raw[i] = metricJSON(base + float64(i))
	}
	if accepted := s.store(key, raw, now); accepted != metricMaxBatch {
		t.Fatal("batch was not capped")
	}
	if got := s.read(key, base+metricMaxBatch, now); len(got) != 0 {
		t.Fatal("processed beyond first 500 samples")
	}
	for start := metricMaxBatch; start < metricMaxPerDevice+500; start += metricMaxBatch {
		batch := make([]json.RawMessage, metricMaxBatch)
		for i := range batch {
			batch[i] = metricJSON(base + float64(start+i))
		}
		s.store(key, batch, now)
	}
	got := s.read(key, 0, now)
	if len(got) != metricMaxPerDevice || got[0].ObservedAt != base+500 || got[len(got)-1].ObservedAt != base+3499 || s.total != len(got) {
		t.Fatalf("capacity/ordering mismatch: len=%d total=%d", len(got), s.total)
	}
}

func TestMetricGlobalCapacity(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	for _, byDevices := range []bool{false, true} {
		t.Run(fmt.Sprintf("devices=%v", byDevices), func(t *testing.T) {
			s := testMetricStore(t)
			count, perDevice := metricMaxTotal/metricMaxPerDevice, metricMaxPerDevice
			if byDevices {
				count, perDevice = metricMaxDevices, 1
			}
			for i := 0; i < count; i++ {
				samples := make([]metricSample, perDevice)
				for j := range samples {
					samples[j].ObservedAt = float64(now.Add(-time.Hour).UnixMilli()) + float64(i)
				}
				s.series[metricKey{"u1", fmt.Sprint(i)}] = samples
				s.total += len(samples)
			}
			s.store(metricKey{"u2", "new"}, []json.RawMessage{metricJSON(float64(now.UnixMilli()))}, now)
			if s.total > metricMaxTotal || len(s.series) > metricMaxDevices {
				t.Fatal("global capacity exceeded")
			}
			if _, ok := s.series[metricKey{"u1", "0"}]; ok {
				t.Fatal("oldest device history was not evicted")
			}
			if got := s.read(metricKey{"u2", "new"}, 0, now); len(got) != 1 {
				t.Fatal("new history missing")
			}
		})
	}
}

func TestMetricCleanupWithoutTraffic(t *testing.T) {
	s := testMetricStore(t)
	s.cleanupInterval = 5 * time.Millisecond
	now := time.Now().Truncate(time.Millisecond)
	s.store(metricKey{"offline", "d1"}, []json.RawMessage{metricJSON(float64(now.Add(-metricRetention + 25*time.Millisecond).UnixMilli()))}, now)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		empty := len(s.series) == 0 && s.total == 0 && s.timer == nil
		s.mu.Unlock()
		if empty {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("idle history was not cleaned without incoming traffic")
}

func TestDeviceMetricsWebSocketAndHTTP(t *testing.T) {
	s := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(s.Routes())
	defer ts.Close()
	t.Cleanup(func() {
		s.metrics.mu.Lock()
		defer s.metrics.mu.Unlock()
		if s.metrics.timer != nil {
			s.metrics.timer.Stop()
		}
	})
	device := dialTestWS(t, ts.URL, "/ws?userId=u1&deviceId=d1")
	defer device.close()
	at := float64(time.Now().UnixMilli())
	sample := metricJSON(at)
	// Unauthenticated data must neither be stored nor acknowledged.
	device.sendJSON(t, map[string]any{"type": "device_metrics", "batchId": "before-auth", "samples": []json.RawMessage{sample}})
	device.sendJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	assertWSJSON(t, device, map[string]any{"type": "auth_success"})
	res := postJSON(t, ts.URL+"/api/device/metrics", "service-token", `{"userId":"u1","deviceId":"d1"}`)
	assertJSON(t, res, map[string]any{"samples": []any{}})
	for i := 0; i < 2; i++ {
		device.sendJSON(t, map[string]any{"type": "device_metrics", "batchId": "batch-1", "deviceId": "spoofed", "userId": "spoofed", "samples": []json.RawMessage{sample}})
		assertWSJSON(t, device, map[string]any{"type": "device_metrics_ack", "batchId": "batch-1", "accepted": float64(1)})
	}
	device.sendJSON(t, map[string]any{"type": "device_metrics", "batchId": "bad-batch", "samples": "invalid"})
	assertWSJSON(t, device, map[string]any{"type": "device_metrics_ack", "batchId": "bad-batch", "accepted": float64(0)})
	device.sendJSON(t, map[string]any{"type": "heartbeat"})
	assertWSJSON(t, device, map[string]any{"type": "heartbeat_ack"})
	device.close()
	for _, query := range []string{`{"userId":"u1","deviceId":"d1"}`, fmt.Sprintf(`{"userId":"u1","deviceId":"d1","since":%v}`, at), `{"userId":"u1","deviceId":"d1","since":"ignored"}`} {
		res := postJSON(t, ts.URL+"/api/device/metrics", "service-token", query)
		assertStatus(t, res, 200)
		var body struct {
			Samples []metricSample `json:"samples"`
		}
		decodeJSON(t, res, &body)
		want, ok := decodeMetricSample(sample)
		if !ok || !reflect.DeepEqual(body.Samples, []metricSample{want}) {
			t.Fatalf("offline query mismatch: %+v", body)
		}
	}
	for _, query := range []string{`{"userId":"u2","deviceId":"d1"}`, `{"userId":"u1","deviceId":"spoofed"}`} {
		res := postJSON(t, ts.URL+"/api/device/metrics", "service-token", query)
		assertJSON(t, res, map[string]any{"samples": []any{}})
	}
	res = postJSON(t, ts.URL+"/api/device/metrics", "wrong", `{"userId":"u1","deviceId":"d1"}`)
	assertStatus(t, res, http.StatusUnauthorized)
	res.Body.Close()
	res = postJSON(t, ts.URL+"/api/device/metrics", "service-token", `{"userId":"u1"}`)
	assertStatus(t, res, 400)
	assertBody(t, res, "Missing deviceId")
	res = postJSON(t, ts.URL+"/api/device/metrics", "service-token", `{"deviceId":"d1"}`)
	assertStatus(t, res, 400)
	assertBody(t, res, "Missing userId")
	fresh := NewServer(Config{ServiceToken: "service-token"})
	if got := fresh.metrics.read(metricKey{"u1", "d1"}, 0, time.Now()); len(got) != 0 {
		t.Fatal("new process retained metric history")
	}
}
