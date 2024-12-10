// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tracer.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include <opentelemetry/context/context.h>
#include <opentelemetry/sdk/trace/samplers/always_off.h>

#ifdef HAVE_JAEGER
#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/samplers/parent.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/span_startoptions.h"

namespace tracing {

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> Tracer::noop_tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("no-op", OPENTELEMETRY_SDK_VERSION);
const jspan Tracer::noop_span = noop_tracer->StartSpan("noop");

using bufferlist = ceph::buffer::list;

Tracer::Tracer(opentelemetry::nostd::string_view service_name) {
  init(service_name);
}

void Tracer::init(opentelemetry::nostd::string_view service_name) {
  if (!tracer) {
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> provider;
    const opentelemetry::sdk::trace::BatchSpanProcessorOptions processor_options;

    using namespace opentelemetry;

    if (g_ceph_context->_conf->otlp_tracing_enable) {

      // Select OTLP (OpenTelemetry Protocol) and configure for gRPC traces.
      opentelemetry::exporter::otlp::OtlpGrpcExporterOptions otlp_exporter_options;
      if (g_ceph_context) {
        otlp_exporter_options.endpoint = g_ceph_context->_conf->otlp_endpoint_url;
        otlp_exporter_options.use_ssl_credentials = false; // XXX !!!
      }
      const auto otlp_resource = sdk::resource::Resource::Create(std::move(
          sdk::resource::ResourceAttributes{{"service.name", service_name}}));
      auto exporter = std::unique_ptr<sdk::trace::SpanExporter>(
          new exporter::otlp::OtlpGrpcExporter(otlp_exporter_options));

      namespace sdktrace = ::opentelemetry::sdk::trace;

      std::unique_ptr<sdktrace::Sampler> sampler;
      if (!g_ceph_context->_conf->otlp_sampler_parent_based) {
        // The ParentBasedSampler delegate is not used.
        sampler =
            std::unique_ptr<sdktrace::Sampler>(new sdktrace::AlwaysOnSampler());
      } else {
        // The ParentBasedSampler delegate (fallback) is configured using option
        // 'otlp_sampler_delegate_defaults_to_on'.
        std::unique_ptr<sdktrace::Sampler> delegate_sampler;
        if (g_ceph_context->_conf->otlp_sampler_delegate_defaults_to_on) {
          delegate_sampler = std::unique_ptr<sdktrace::Sampler>(
              new sdktrace::AlwaysOnSampler());
        } else {
          delegate_sampler = std::unique_ptr<sdktrace::Sampler>(
              new sdktrace::AlwaysOffSampler());
        }
        sampler = std::unique_ptr<sdktrace::Sampler>(
            new sdktrace::ParentBasedSampler(std::move(delegate_sampler)));
      }

      auto processor = std::unique_ptr<sdk::trace::SpanProcessor>(
          new sdktrace::BatchSpanProcessor(std::move(exporter),
                                           processor_options));
      provider =
          nostd::shared_ptr<trace::TracerProvider>(new sdktrace::TracerProvider(
              std::move(processor), otlp_resource, std::move(sampler)));

    } else {
      // Default is a Jaeger trace.
      exporter::jaeger::JaegerExporterOptions exporter_options;
      if (g_ceph_context) {
        exporter_options.endpoint = g_ceph_context->_conf.get_val<std::string>("jaeger_agent_host");
        exporter_options.server_port = g_ceph_context->_conf.get_val<int64_t>("jaeger_agent_port");
      }
      const auto jaeger_resource = sdk::resource::Resource::Create(std::move(
          sdk::resource::ResourceAttributes{{"service.name", service_name}}));
      auto exporter = std::unique_ptr<sdk::trace::SpanExporter>(
          new exporter::jaeger::JaegerExporter(exporter_options));

      auto processor = std::unique_ptr<sdk::trace::SpanProcessor>(
          new sdk::trace::BatchSpanProcessor(std::move(exporter),
                                             processor_options));
      provider = nostd::shared_ptr<trace::TracerProvider>(
          new sdk::trace::TracerProvider(std::move(processor),
                                         jaeger_resource));
    }

    trace::Provider::SetTracerProvider(provider);
    tracer = provider->GetTracer(service_name, OPENTELEMETRY_SDK_VERSION);

    // Set global propagator
    context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
        nostd::shared_ptr<context::propagation::TextMapPropagator>(
            new trace::propagation::HttpTraceContext()));
  }
}

jspan Tracer::start_trace(opentelemetry::nostd::string_view trace_name) {
  if (is_enabled()) {
    return tracer->StartSpan(trace_name);
  }
  return noop_span;
}

jspan Tracer::start_trace(opentelemetry::nostd::string_view trace_name, bool trace_is_enabled) {
  if (trace_is_enabled) {
    return tracer->StartSpan(trace_name);
  }
  return noop_tracer->StartSpan(trace_name);
}

jspan Tracer::start_trace_with_req_state_parent(opentelemetry::nostd::string_view trace_name,
    bool trace_is_enabled,
    const std::string& traceparent_header, const std::string& tracestate_header)
{
  if (!trace_is_enabled) {
    return noop_tracer->StartSpan(trace_name);
  }

  using namespace opentelemetry;

  // Get global propagator
  HttpTextMapCarrier<opentelemetry::ext::http::client::Headers> carrier;
  carrier.Set("traceparent", traceparent_header);
  carrier.Set("tracestate", tracestate_header);
  auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();

  // Extract headers to context
  auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
  auto new_context = propagator->Extract(carrier, current_ctx);
  auto remote_span = opentelemetry::trace::GetSpan(new_context);

  trace::StartSpanOptions span_opts;
  span_opts.parent = remote_span->GetContext();

  return tracer->StartSpan(trace_name, {}, span_opts);
}

jspan Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan& parent_span) {
  if (is_enabled() && parent_span->IsRecording()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_span->GetContext();
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

jspan Tracer::add_span(opentelemetry::nostd::string_view span_name, const jspan_context& parent_ctx) {
  if (is_enabled() && parent_ctx.IsValid()) {
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    return tracer->StartSpan(span_name, span_opts);
  }
  return noop_span;
}

bool Tracer::is_enabled() const {
  return g_ceph_context->_conf->jaeger_tracing_enable || g_ceph_context->_conf->otlp_tracing_enable;
}

void encode(const jspan_context& span_ctx, bufferlist& bl, uint64_t f) {
  ENCODE_START(1, 1, bl);
  using namespace opentelemetry;
  using namespace trace;
  auto is_valid = span_ctx.IsValid();
  encode(is_valid, bl);
  if (is_valid) {
    encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.trace_id().Id().data()), TraceId::kSize), bl);
    encode_nohead(std::string_view(reinterpret_cast<const char*>(span_ctx.span_id().Id().data()), SpanId::kSize), bl);
    encode(span_ctx.trace_flags().flags(), bl);
  }
  ENCODE_FINISH(bl);
}

void decode(jspan_context& span_ctx, bufferlist::const_iterator& bl) {
  using namespace opentelemetry;
  using namespace trace;
  DECODE_START(1, bl);
  bool is_valid;
  decode(is_valid, bl);
  if (is_valid) {
    std::array<uint8_t, TraceId::kSize> trace_id;
    std::array<uint8_t, SpanId::kSize> span_id;
    uint8_t flags;
    decode(trace_id, bl);
    decode(span_id, bl);
    decode(flags, bl);
    span_ctx = SpanContext(
      TraceId(nostd::span<uint8_t, TraceId::kSize>(trace_id)),
      SpanId(nostd::span<uint8_t, SpanId::kSize>(span_id)),
      TraceFlags(flags),
      true);
  }
  DECODE_FINISH(bl);
}
} // namespace tracing

#endif // HAVE_JAEGER
