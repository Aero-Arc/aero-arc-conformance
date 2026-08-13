# Contributing

Contributions are welcome under the Mozilla Public License 2.0.

Before opening a pull request:

```bash
make build test test-race vet integration
```

Update configuration and design documentation whenever a contract or invariant
changes. Add MPL 2.0 headers to new Go files. Integration tests must perform
real operations against their containers rather than asserting readiness alone.

All commits require Developer Certificate of Origin sign-off:

```bash
git commit -s -m "Describe the change"
```
