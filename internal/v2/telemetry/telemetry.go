package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Setup installs an OTLP trace provider only when tracing is explicitly
// enabled. The exporter itself follows the standard OTEL_EXPORTER_OTLP_*
// environment contract. Metrics remain on the existing bounded Prometheus
// endpoints and are not duplicated through OpenTelemetry.
func Setup(ctx context.Context, role, cluster string) (func(context.Context) error, error) {
	exporterName := strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_TRACES_EXPORTER")))
	if exporterName == "" || exporterName == "none" {
		return func(context.Context) error { return nil }, nil
	}
	if exporterName != "otlp" {
		return nil, fmt.Errorf("OTEL_TRACES_EXPORTER must be otlp, none or empty")
	}
	protocol := strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")))
	if protocol == "" {
		protocol = strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol != "" && protocol != "http/protobuf" {
		return nil, fmt.Errorf("OTLP tracing supports only the http/protobuf protocol")
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OpenTelemetry span exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName("icinga-kubernetes"),
			attribute.String("service.namespace", "icinga"),
			attribute.String("service.instance.role", role),
			attribute.String("icinga.kubernetes.cluster", cluster),
		),
	)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
