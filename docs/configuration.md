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
- telemetry overlap rereads recent evidence on restart and preserves a
  same-timestamp suffix after the checkpoint cursor; it does not yet reconcile
  newly visible observations older than that cursor;
- settle delay trades live latency for reduced visibility reordering;
- freshness governs monitoring availability, not geometric containment;
- query `max_rows` is a completeness guard, not a performance target.
- Registry request timeout must be shorter than its outbox lease duration;
- Registry publication and retry intervals control delivery cadence without
  coupling evaluation commits to Registry availability.

The example's one-second Influx poll and Registry publication intervals are
prototype polling defaults, not supported fleet-throughput targets. Today every
live evaluation commit produces a checkpoint and Registry outbox row, and the
example publisher claims at most 20 rows per flush. The planned production
controls separate evaluation, recovery checkpoint, projection-change,
heartbeat, final-reconciliation, archival, and retention cadences; they must not
be added to configuration before their behavior and safety relationships are
implemented and tested. See
[Scaling, Retention, and Flight Finalization](scaling-retention.md).

`service.grpc_address` exposes assignment lifecycle RPCs and always requires
mutual TLS. `service.grpc_tls.certificate_file` and `private_key_file` identify
the Conformance server identity. `client_ca_file` must contain the CA used to
verify API client certificates; use a dedicated API-client CA so another
workload certificate cannot authorize assignment lifecycle changes. The
request `source` remains an idempotency namespace and is not an authentication
credential. Certificate changes take effect after a service restart.

`policy.version` is also an assignment-ingress compatibility fence. Conformance
rejects a prepared assignment whose policy version differs from the policy
loaded by the running process, rather than accepting work its evaluator cannot
execute. Configuration is loaded at startup, so changing the accepted policy
version requires a controlled service restart.

The evaluator claims up to `worker.claim_batch_size` due assignments per
`influx.poll_interval`. It queries only through `now - influx.settle_delay`,
recursively splits any saturated half-open window, renews ownership every
`worker.renew_interval`, and commits the next evaluation due time only with a
complete non-empty suffix. Empty or failed reads release and reschedule the
claim without advancing the durable checkpoint or evaluation revision. They do
not yet publish a monitoring-only stale/unavailable revision; Registry TTL is
the current fallback.
After an authority interval ends, it remains claimable for the settle delay plus
one poll interval so at least one scheduled query can reach the final
`[last checkpoint, authority_until)` tail. This grace extends processing
eligibility, never assignment authority.

`registry.address` selects the Registry gRPC target; `registry.insecure` is
intended only for trusted development networks. Production deployments should
also use authenticated Registry transport and network policy appropriate to
their environment.
