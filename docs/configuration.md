# Configuration

Copy `configs/config.yaml.example` to the ignored `configs/config.yaml`. Values
support `${ENVIRONMENT_VARIABLE}` expansion. Unknown YAML keys fail startup so
configuration typos cannot silently select defaults.

Sensitive values such as PostgreSQL credentials and Influx tokens should enter
through environment variables or mounted secret files, never source control.

Important timing relationships:

- worker renewal interval must be shorter than lease duration;
- telemetry overlap must cover expected delayed visibility and restart delay;
- settle delay trades live latency for reduced visibility reordering;
- freshness governs monitoring availability, not geometric containment;
- query `max_rows` is a completeness guard, not a performance target.
- Registry request timeout must be shorter than its outbox lease duration;
- Registry publication and retry intervals control delivery cadence without
  coupling evaluation commits to Registry availability.

`service.grpc_address` exposes assignment lifecycle RPCs. `registry.address`
selects the Registry gRPC target; `registry.insecure` is intended only for
trusted development networks. Production deployments should use transport
credentials and network policy appropriate to their environment.
