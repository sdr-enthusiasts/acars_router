// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! Shared async DNS resolver built on top of `hickory-resolver`.
//!
//! The connection manager creates one [`Resolver`] (an [`Arc`]) and threads it
//! through to every consumer that needs to resolve a hostname. This replaces
//! two long-standing bugs:
//!
//! * `tcp_services::TCPReceiverServer::run` used to call
//!   `host.parse::<SocketAddr>()` on a `host:port` string, which rejected every
//!   non-literal hostname.
//! * `udp_services::UDPSenderServer::send_bytes` used to call
//!   `std::net::ToSocketAddrs::to_socket_addrs()` on the tokio runtime, which
//!   performed a *blocking* libc DNS lookup on the async runtime thread.
//!
//! Both call sites now go through this module.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{CLOUDFLARE, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use log::warn;

/// The concrete resolver type used throughout the crate.
pub type Resolver = TokioResolver;

/// Construct a new shared resolver.
///
/// Tries the operating system configuration first (`/etc/resolv.conf` on Unix,
/// the registry on Windows). If that fails, falls back to Cloudflare's
/// public resolver (1.1.1.1 / 1.0.0.1) and emits a warning.
#[must_use]
pub fn new_shared_resolver() -> Arc<Resolver> {
    let resolver = match TokioResolver::builder_tokio() {
        Ok(builder) => builder.build().map_err(|e| {
            warn!(
                "failed to build system-configured DNS resolver: {e}; falling back to Cloudflare"
            );
            e
        }),
        Err(e) => {
            warn!("failed to read system DNS configuration: {e}; falling back to Cloudflare");
            Err(e)
        }
    };

    let resolver = resolver.unwrap_or_else(|_| {
        TokioResolver::builder_with_config(
            ResolverConfig::udp_and_tcp(&CLOUDFLARE),
            TokioRuntimeProvider::default(),
        )
        .with_options(ResolverOpts::default())
        .build()
        .expect("cloudflare ResolverConfig must build")
    });

    Arc::new(resolver)
}

/// Resolve a `host:port` string to a single [`SocketAddr`].
///
/// Behaviour:
/// * If the host portion is an IP literal (with or without brackets for IPv6),
///   no DNS lookup happens and the address is returned directly.
/// * Otherwise the resolver is consulted and the first IPv4 result is
///   preferred. If no IPv4 result is returned, the first IPv6 result is used.
///
/// Returns `io::Error` of kind `InvalidInput` when the port is missing or not
/// a valid `u16`, and `io::Error` of kind `NotFound`/`Other` when DNS fails.
pub async fn resolve_host_port(
    resolver: &Resolver,
    host_port: &str,
) -> std::io::Result<SocketAddr> {
    let (host, port_str) = host_port.rsplit_once(':').ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("missing port in `{host_port}`"),
        )
    })?;
    let port: u16 = port_str.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid port `{port_str}`: {e}"),
        )
    })?;

    // Strip brackets for bracketed IPv6 literals like `[::1]`.
    let host_stripped = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);

    if let Ok(ip) = host_stripped.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    let ip = resolve_first_ip(resolver, host).await?;
    Ok(SocketAddr::new(ip, port))
}

async fn resolve_first_ip(resolver: &Resolver, host: &str) -> std::io::Result<IpAddr> {
    let lookup = resolver
        .lookup_ip(host)
        .await
        .map_err(|e| std::io::Error::other(format!("dns lookup for `{host}` failed: {e}")))?;

    let mut first_v6: Option<IpAddr> = None;
    for ip in lookup.iter() {
        if ip.is_ipv4() {
            return Ok(ip);
        }
        if first_v6.is_none() {
            first_v6 = Some(ip);
        }
    }

    first_v6.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no A/AAAA records returned for `{host}`"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    fn resolver() -> Arc<Resolver> {
        new_shared_resolver()
    }

    #[tokio::test]
    async fn resolves_ipv4_literal_without_network() {
        let r = resolver();
        let addr = resolve_host_port(&r, "127.0.0.1:8080").await.unwrap();
        assert_eq!(addr, "127.0.0.1:8080".parse().unwrap());
    }

    #[tokio::test]
    async fn resolves_bracketed_ipv6_literal_without_network() {
        let r = resolver();
        let addr = resolve_host_port(&r, "[::1]:8080").await.unwrap();
        assert_eq!(addr, SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8080));
    }

    #[tokio::test]
    async fn errors_when_no_port() {
        let r = resolver();
        let err = resolve_host_port(&r, "bad").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn errors_when_port_is_not_numeric() {
        let r = resolver();
        let err = resolve_host_port(&r, "host:abc").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
