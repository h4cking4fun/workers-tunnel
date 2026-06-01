# Worker Tunnel Guide

This document describes the tunnel architecture, supported proxy modes, client configuration, and fallback SOCKS5 server configuration for `workers-tunnel`.

The project exposes a VLESS-over-WebSocket endpoint on Cloudflare Workers. TCP traffic is routed direct-first from the Worker and falls back to configured outbound targets. UDP is handled through DNS-over-HTTPS for DNS traffic and through sing-box UDP-over-TCP for non-DNS UDP when a SOCKS5 fallback is configured.

## Architecture

```mermaid
flowchart LR
    Client["VLESS client<br/>mihomo / sing-box / Xray"] -->|"VLESS over TLS WebSocket"| Worker["Cloudflare Worker"]
    Worker -->|"TCP direct first"| Internet["Target site"]
    Worker -->|"TCP fallback via SOCKS5 CONNECT"| Socks["Fallback SOCKS5 server"]
    Worker -->|"UDP/53 DNS-over-HTTPS"| DoH["1.1.1.1 DNS-over-HTTPS"]
    Worker -->|"non-DNS UDP via SOCKS5 UoT v2"| Socks
    Worker -->|"/relay pass-through"| Backend["Backend WebSocket proxy"]
    Backend -->|"proxy protocol / optional smux"| Internet
    Socks -->|"Direct TCP/UDP egress"| Internet
```

Main modules:

- `src/lib.rs`: Worker fetch entrypoint, environment variable loading, WebSocket upgrade, and tunnel startup.
- `src/config.rs`: Worker config parsing, including `USER_ID`, early data, and `PROXY_IP`.
- `src/passthrough.rs`: `/relay` WebSocket pass-through to a backend proxy server.
- `src/protocol.rs`: VLESS request parsing for TCP, raw UDP, and XUDP.
- `src/proxy.rs`: High-level tunnel dispatch and direct-failure cache.
- `src/relay.rs`: TCP relay, DNS-over-HTTPS UDP, raw UDP to UoT, and XUDP to UoT.
- `src/socks5.rs`: SOCKS5 TCP CONNECT client.
- `src/uot.rs`: sing-box UDP-over-TCP v2 framing.
- `src/xudp.rs`: mihomo/sing VLESS XUDP frame parsing and response encoding.

## Request Flow

### TCP

For VLESS `cmd=1`, the Worker tries:

1. The original requested destination directly.
2. Each `PROXY_IP` entry in order.

Plain `PROXY_IP` entries are direct fallback targets. SOCKS5 entries use SOCKS5 TCP CONNECT to the original requested destination.

Direct connection failures are cached by `host:port` for 30 minutes per Worker isolate. While cached, the Worker skips the direct attempt and tries configured fallbacks immediately.

### UDP/53

For VLESS `cmd=2` with destination port `53`, the Worker keeps DNS inside the Worker and sends the DNS payload to Cloudflare DNS-over-HTTPS:

```text
https://1.1.1.1/dns-query
```

This path does not use the SOCKS5 fallback.

### Raw UDP

For VLESS `cmd=2` with destination port other than `53`, the Worker cannot send arbitrary UDP directly. It selects SOCKS5 entries from `PROXY_IP` and opens sing-box UDP-over-TCP v2 through that SOCKS5 server.

Plain direct `PROXY_IP` entries are ignored for non-DNS UDP because Cloudflare Workers do not expose arbitrary UDP sockets.

### XUDP

For VLESS `cmd=3`, mihomo may send XUDP frames even when `smux.enabled=false`. The Worker parses XUDP frames, extracts the UDP destination from each mux frame, and bridges packet payloads to the same SOCKS5 UoT backend.

The Worker supports XUDP for UDP only. TCP mux streams are rejected because the tunnel already supports normal VLESS TCP without implementing a full generic VLESS mux server.

### Relay Pass-Through

Requests to `/relay` use a separate pass-through mode. The Worker does not parse VLESS, `USER_ID`, `PROXY_IP`, early data, XUDP, or UoT on this path. It bridges bytes between the client WebSocket and `RELAY_BACKEND_URL`, so any compatible backend WebSocket proxy can own the actual proxy protocol.

Use `/relay` when you want a backend proxy server to own the WebSocket protocol and any optional smux/multiplex session management. Use `/ws?ed=512` when you want the Worker-native direct-first and SOCKS5 fallback behavior.

## Worker Configuration

Configure these Worker variables:

| Variable | Required | Sensitive | Purpose |
| --- | --- | --- | --- |
| `USER_ID` | Yes | Yes | VLESS UUID accepted by the Worker. |
| `PROXY_IP` | No | Yes if SOCKS5 auth is used | Whitespace-separated direct and SOCKS5 fallback targets. |
| `FALLBACK_SITE` | No | No | Non-WebSocket HTTP fallback URL. |
| `SHOW_URI` | No | No | If `true`, requests containing `USER_ID` return a generated VLESS URI. |
| `DEBUG_LOG` | No | No | Enables per-attempt and first-response tunnel logs. Use only while debugging. |
| `RELAY_BACKEND_URL` | No | Yes if the URL contains private topology or credentials | Backend WSS URL for `/relay` pass-through. |

Example `wrangler.toml` local values:

```toml
[vars]
USER_ID = "00000000-0000-0000-0000-000000000000"
SHOW_URI = "true"
PROXY_IP = "socks5://user:pass@proxy.example.com:1080"
FALLBACK_SITE = ""
DEBUG_LOG = "false"
RELAY_BACKEND_URL = "wss://backend.example.com/vless"
```

For GitHub Actions deployment, store `USER_ID`, `PROXY_IP`, and `RELAY_BACKEND_URL` as GitHub repository secrets. `PROXY_IP` is sensitive when it contains inline SOCKS5 credentials.

## `PROXY_IP` Grammar

`PROXY_IP` remains whitespace-separated:

```text
PROXY_IP="1.2.3.4 1.2.3.4:443 socks5://proxy.example.com:1080 socks5://user:pass@proxy.example.com:1080"
```

Supported tokens:

| Token | Meaning |
| --- | --- |
| `host` | Direct TCP fallback to `host` using the original destination port. |
| `host:port` | Direct TCP fallback to `host:port`. |
| `socks5://host:port` | SOCKS5 fallback without authentication. |
| `socks5://user:pass@host:port` | SOCKS5 fallback with username/password authentication. |

Credentials must not contain whitespace. Percent-encoded URI credentials are decoded before SOCKS5 authentication.

Routing behavior:

- TCP uses the original destination first, then all `PROXY_IP` entries in order.
- UDP/53 uses DNS-over-HTTPS and does not use `PROXY_IP`.
- Non-DNS raw UDP and XUDP use only SOCKS5 `PROXY_IP` entries.
- SOCKS5 logs are sanitized as `socks5://host:port`; credentials are not logged.

## Client Configuration

### mihomo

Recommended Worker-native proxy entry:

```yaml
proxies:
  - name: workers-tunnel
    type: vless
    server: cf.example.com
    port: 443
    uuid: 00000000-0000-0000-0000-000000000000
    udp: true
    tls: true
    servername: cf.example.com
    client-fingerprint: chrome
    network: ws
    ws-opts:
      path: /ws?ed=512
      headers:
        Host: cf.example.com
```

Notes:

- Keep `udp: true`.
- `packet-encoding` can be omitted. mihomo can use XUDP, and the Worker supports it.
- `packet-encoding: ""` can be used only when intentionally testing raw VLESS UDP `cmd=2`.
- `smux.enabled=false` is not required for UDP support. Disable smux only if it causes separate TCP multiplexing issues in your environment.
- Use rules to avoid sending all device traffic through a free-plan Worker. Route only the traffic that benefits from this tunnel.

Backend relay pass-through profile:

```yaml
proxies:
  - name: workers-tunnel-relay
    type: vless
    server: cf.example.com
    port: 443
    uuid: 00000000-0000-0000-0000-000000000000
    udp: true
    tls: true
    servername: cf.example.com
    client-fingerprint: chrome
    network: ws
    ws-opts:
      path: /relay
      headers:
        Host: cf.example.com
    smux:
      enabled: true
      protocol: smux
      only-tcp: true
```

In this profile, the Worker forwards the WebSocket stream to `RELAY_BACKEND_URL`. The backend validates the VLESS UUID and may handle smux. Keep `only-tcp: true` if UDP should continue to use the Worker-native XUDP/UoT profile.

### Xray

Basic VLESS WebSocket outbound shape:

```json
{
  "protocol": "vless",
  "settings": {
    "vnext": [
      {
        "address": "cf.example.com",
        "port": 443,
        "users": [
          {
            "id": "00000000-0000-0000-0000-000000000000",
            "encryption": "none"
          }
        ]
      }
    ]
  },
  "streamSettings": {
    "network": "ws",
    "security": "tls",
    "tlsSettings": {
      "serverName": "cf.example.com",
      "fingerprint": "chrome"
    },
    "wsSettings": {
      "path": "/ws?ed=512",
      "headers": {
        "Host": "cf.example.com"
      }
    }
  }
}
```

## Fallback SOCKS5 Server

The fallback server should be a TCP-reachable SOCKS5 server. For non-DNS UDP support, use sing-box as the SOCKS5 server so it can handle UDP-over-TCP v2 magic-address requests from the Worker.

Example sing-box server config:

```json
{
  "log": {
    "level": "info",
    "timestamp": true
  },
  "inbounds": [
    {
      "type": "socks",
      "tag": "socks-in",
      "listen": "0.0.0.0",
      "listen_port": 1080,
      "users": [
        {
          "username": "CHANGE_ME_USER",
          "password": "CHANGE_ME_PASSWORD"
        }
      ]
    }
  ],
  "outbounds": [
    {
      "type": "direct",
      "tag": "direct"
    }
  ]
}
```

Then configure the Worker:

```text
PROXY_IP="socks5://CHANGE_ME_USER:CHANGE_ME_PASSWORD@socks.example.com:1080"
```

Operational requirements:

- The SOCKS5 listen port must be reachable from Cloudflare Workers egress.
- The server firewall must allow the configured TCP port.
- Use strong unique credentials.
- Do not publish `PROXY_IP` values with credentials in logs, issues, commits, or screenshots.
- Run `sing-box check -c config.json` before starting the service.

The Worker uses sing-box UoT v2 by opening SOCKS5 CONNECT to the magic address `sp.v2.udp-over-tcp.arpa:0`, then sending UoT v2 connect-stream datagram frames.

## Backend Relay Server

The `/relay` path requires a backend WebSocket proxy server that speaks the same protocol as the client. A common setup is sing-box VLESS with optional smux on localhost behind Caddy or Nginx TLS.

Example sing-box backend:

```json
{
  "log": {
    "level": "info",
    "timestamp": true
  },
  "inbounds": [
    {
      "type": "vless",
      "tag": "vless-ws-in",
      "listen": "127.0.0.1",
      "listen_port": 10000,
      "users": [
        {
          "name": "worker-user",
          "uuid": "00000000-0000-0000-0000-000000000000"
        }
      ],
      "multiplex": {
        "enabled": true,
        "padding": false
      },
      "transport": {
        "type": "ws",
        "path": "/vless"
      }
    }
  ],
  "outbounds": [
    {
      "type": "direct",
      "tag": "direct"
    }
  ]
}
```

Example Caddy reverse proxy:

```caddyfile
{
  admin off
  auto_https disable_redirects
}

http://backend.example.com {
  @vless path /vless
  handle @vless {
    reverse_proxy 127.0.0.1:10000
  }

  handle {
    redir https://{host}{uri} 308
  }
}

https://backend.example.com {
  @vless path /vless
  handle @vless {
    reverse_proxy 127.0.0.1:10000
  }
}
```

Then configure the Worker secret:

```text
RELAY_BACKEND_URL="wss://backend.example.com/vless"
```

Operational notes:

- Do not configure client WebSocket early data on `/relay`; use `path: /relay`, not `/relay?ed=512`.
- The Worker does not authenticate `/relay` traffic. The backend VLESS server must validate the UUID.
- `RELAY_BACKEND_URL` logs are sanitized before printing, but avoid embedding credentials in the backend URL.
- The Worker backend WebSocket is a Fetch upgrade subrequest. Some backends receive it on HTTP with `X-Forwarded-Proto: https`; make `/vless` bypass HTTP-to-HTTPS redirects before any fallback `redir`.
- In Caddy, put the proxy and fallback redirect in separate `handle` blocks. A bare `redir` beside `handle /vless` is not mutually exclusive and can still redirect `/vless`.
- Use backend logs to confirm VLESS inbound and multiplex sessions.

## Debugging

Set `DEBUG_LOG=true` temporarily to verify routing. Expected XUDP success markers:

```text
xudp route selected: socks5://proxy.example.com:1080 -> target.example.com:443
uot connected: socks5://proxy.example.com:1080 -> target.example.com:443
xudp first response: 1200 bytes from target.example.com:443 via socks5://proxy.example.com:1080
```

Expected raw UDP success markers:

```text
connect udp via UoT: socks5://proxy.example.com:1080
uot connected: socks5://proxy.example.com:1080 -> target.example.com:443
uot first response: 1200 bytes via socks5://proxy.example.com:1080
```

Expected relay pass-through markers:

```text
relay backend connect: wss://backend.example.com/vless
relay backend connected: wss://backend.example.com/vless
relay stream ended: wss://backend.example.com/vless
```

Turn `DEBUG_LOG=false` after validation. Long-lived WebSocket tunnels plus verbose logging can contribute to Cloudflare Free plan `exceededCpu` and `loadShed` outcomes.

Common Cloudflare outcomes:

| Outcome | Meaning |
| --- | --- |
| `ok` | Request completed normally. |
| `canceled` | Often normal for long-lived tunnel/WebSocket requests closed by the client. |
| `exceededCpu` | Worker exceeded CPU limits. Reduce routed traffic, disable debug logs, or move heavy traffic off Worker. |
| `loadShed` | Cloudflare dropped the request under platform or account pressure. This is not a Rust error from the Worker. |

## Limits And Tradeoffs

- Cloudflare Workers cannot open arbitrary UDP sockets. Non-DNS UDP requires the SOCKS5 UoT backend.
- UoT carries UDP over TCP. It improves compatibility but can suffer from TCP head-of-line blocking for latency-sensitive UDP.
- The direct-failure cache is in-memory per Worker isolate. It resets on cold start and deploy.
- Worker-native `/ws` is still one normal VLESS stream. Use `/relay` with a backend VLESS server when you need backend-managed TCP multiplexing.
- Free-plan Workers are fragile for high-volume or always-on proxy workloads. Use routing rules to limit traffic, or run a dedicated VPS proxy when reliability matters.

## References

- [sing-box UDP over TCP](https://sing-box.sagernet.org/configuration/shared/udp-over-tcp/)
- [sing-box SOCKS inbound](https://sing-box.sagernet.org/configuration/inbound/socks/)
- [sing-box SOCKS outbound](https://sing-box.sagernet.org/configuration/outbound/socks/)
- [mihomo VLESS configuration](https://wiki.metacubex.one/en/config/proxies/vless/)
