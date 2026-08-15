# Self-hosting the browser client

Build the WASM image from the repository root:

```bash
docker build -f client/wasm/Dockerfile -t cloink-wasm:0.77.0 .
```

Run it behind the same TLS reverse proxy as the Dashboard, or on a dedicated
TLS hostname:

```bash
docker run --rm -p 8082:80 cloink-wasm:0.77.0
```

The image exposes:

- `/netbird.wasm`: browser VPN client used by Web SSH and Web RDP.
- `/wasm_exec.js`: matching Go WASM runtime, useful when hosting both files
  from the same service.
- `/healthz`: container health endpoint.

Set `NETBIRD_WASM_PATH` and `NETBIRD_WASM_EXEC_PATH` on the Dashboard to the
externally reachable URLs, for example `https://wasm.example.com/netbird.wasm`
and `https://wasm.example.com/wasm_exec.js`. Serving the matching runtime avoids
Go toolchain compatibility problems. The Dashboard never falls back to the
NetBird package service when these values are absent.
