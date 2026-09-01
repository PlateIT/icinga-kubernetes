package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

func TestSetupIsDisabledByDefault(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	shutdown, err := Setup(context.Background(), "api", "test")
	if err != nil || shutdown == nil {
		t.Fatalf("disabled setup returned shutdown=%t error=%v", shutdown != nil, err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupRejectsUnknownExporter(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	if shutdown, err := Setup(context.Background(), "api", "test"); err == nil || shutdown != nil {
		t.Fatalf("unknown exporter returned shutdown=%t error=%v", shutdown != nil, err)
	}
}

func TestSetupRejectsUnsupportedProtocolBeforeConnecting(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")
	if shutdown, err := Setup(context.Background(), "api", "test"); err == nil || shutdown != nil {
		t.Fatalf("unsupported protocol returned shutdown=%t error=%v", shutdown != nil, err)
	}
}

func TestSetupExportsOTLPHTTPTrace(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previousProvider)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("unexpected OTLP request path=%q content-type=%q", r.URL.Path, r.Header.Get("Content-Type"))
		}
		requests.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+"/v1/traces")
	shutdown, err := Setup(context.Background(), "api", "test")
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "export-me")
	span.End()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("OTLP requests=%d, want 1", requests.Load())
	}
}
