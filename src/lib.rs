use crate::config::{parse_early_data, parse_outbound_targets, parse_user_id};
use crate::proxy::run_tunnel;
use crate::websocket::WebSocketStream;
use worker::*;

mod config;
mod ext;
mod protocol;
mod proxy;
mod relay;
mod socks5;
mod uot;
mod websocket;
mod xudp;

#[event(fetch)]
async fn main(req: Request, env: Env, _: Context) -> Result<Response> {
    let uuid_str = env.var("USER_ID")?.to_string();

    let is_websocket = req
        .headers()
        .get("Upgrade")?
        .map(|up| up == "websocket")
        .unwrap_or(false);

    if !is_websocket {
        let show_uri: bool = env.var("SHOW_URI")?.to_string().parse().unwrap_or(false);
        if show_uri && req.path().contains(uuid_str.as_str()) {
            let host_str = req.url()?.host_str().unwrap_or_default().to_string();
            let vless_uri = format!(
                "vless://{uuid}@{host}:443?encryption=none&security=tls&sni={host}&fp=chrome&type=ws&host={host}&path=ws#workers-tunnel",
                uuid = uuid_str,
                host = host_str
            );
            return Response::ok(vless_uri);
        }

        let fallback_site = env
            .var("FALLBACK_SITE")
            .map(|v| v.to_string())
            .unwrap_or_default();
        if !fallback_site.is_empty() {
            return Fetch::Url(Url::parse(&fallback_site)?).send().await;
        }

        return Response::ok("ok");
    }

    let user_id = parse_user_id(&uuid_str);

    let proxy_ip = env
        .var("PROXY_IP")
        .map(|proxy_ip| proxy_ip.to_string())
        .unwrap_or_default();
    let proxy_ip = parse_outbound_targets(&proxy_ip).map_err(|err| {
        worker::Error::RustError(format!("invalid PROXY_IP configuration: {}", err))
    })?;
    let debug_log = env
        .var("DEBUG_LOG")
        .map(|value| value.to_string().parse().unwrap_or(false))
        .unwrap_or(false);

    let early_data = req.headers().get("sec-websocket-protocol")?;
    let early_data = parse_early_data(early_data)?;

    let WebSocketPair { client, server } = WebSocketPair::new()?;
    server.accept()?;

    wasm_bindgen_futures::spawn_local(async move {
        let events = match server.events() {
            Ok(events) => events,
            Err(err) => {
                console_error!("error: could not open websocket stream: {}", err);
                _ = server.close(Some(1011), Some("websocket stream error"));
                return;
            }
        };

        let socket = WebSocketStream::new(&server, events, early_data);

        if let Err(err) = run_tunnel(socket, user_id, &proxy_ip, debug_log).await {
            console_error!("error: {}", err);
            _ = server.close(Some(1003), Some("invalid request"));
        }
    });

    Response::from_websocket(client)
}
