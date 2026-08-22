# Configuration

Copy `configs/config.yaml.example` to the ignored `configs/config.yaml`. Values
support `${ENVIRONMENT_VARIABLE}` expansion. Unknown YAML keys fail startup so
configuration typos cannot silently select defaults.

Sensitive values such as PostgreSQL credentials and Influx tokens should enter
through environment variables or mounted secret files, never source control.
The ignored `configs/tls/` directory is the default Compose mount for local
certificates; production deployments should use their secret manager rather
than placing private keys beside the configuration file.

Important timing relationships:

- worker renewal interval must be shorter than lease duration;
- telemetry overlap must cover expected delayed visibility and restart delay;
- settle delay trades live latency for reduced visibility reordering;
- freshness governs monitoring availability, not geometric containment;
- query `max_rows` is a completeness guard, not a performance target.
- Registry request timeout must be shorter than its outbox lease duration;
- Registry publication and retry intervals control delivery cadence without
  coupling evaluation commits to Registry availability.

`service.grpc_address` exposes assignment lifecycle RPCs and always requires
mutual TLS. `service.grpc_tls.certificate_file` and `private_key_file` identify
the Conformance server identity. `client_ca_file` must contain the CA used to
verify API client certificates; use a dedicated API-client CA so another
workload certificate cannot authorize assignment lifecycle changes. The
request `source` remains an idempotency namespace and is not an authentication
credential. Certificate changes take effect after a service restart.

`registry.address` selects the Registry gRPC target; `registry.insecure` is
intended only for trusted development networks. Production deployments should
also use authenticated Registry transport and network policy appropriate to
their environment.
