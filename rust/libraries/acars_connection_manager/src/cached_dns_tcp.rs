// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

//! [`UnderlyingIo`] implementation that re-resolves a `host:port` string on
//! every (re)connect attempt.
//!
//! `sdre-stubborn-io` 0.7.1 deliberately moved DNS out of the crate: the
//! built-in [`TcpStream`] impl takes a `SocketAddr` for `Context`, which means
//! the address is captured once at first connect and reused forever on
//! reconnect. For a long-lived router that connects to dynamic upstreams
//! (round-robin DNS, failover, container restarts) that is exactly the wrong
//! cache lifetime.
//!
//! [`CachedDnsTcp`] preserves the original behaviour by re-running
//! [`crate::dns::resolve_host_port`] inside `establish`, leveraging
//! `hickory-resolver`'s built-in TTL cache so we are not hammering the
//! resolver between rapid reconnect attempts.

use std::io;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use sdre_stubborn_io::tokio::UnderlyingIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::dns::{self, Resolver};

/// TCP keepalive idle/interval applied to every (re)connected stream.
const KEEPALIVE_TIME: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Context handed to [`CachedDnsTcp::establish`] on every (re)connect.
#[derive(Clone)]
pub struct ConnectTarget {
    /// `host:port` string (host may be a name or IP literal, with bracketed
    /// IPv6 supported).
    pub host: Arc<str>,
    pub resolver: Arc<Resolver>,
}

/// Newtype over a freshly-resolved [`TcpStream`].
pub struct CachedDnsTcp(TcpStream);

impl Deref for CachedDnsTcp {
    type Target = TcpStream;
    fn deref(&self) -> &TcpStream {
        &self.0
    }
}

impl UnderlyingIo for CachedDnsTcp {
    type Context = ConnectTarget;

    fn establish(
        ctx: ConnectTarget,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Self>> + Send>> {
        Box::pin(async move {
            let addr = dns::resolve_host_port(&ctx.resolver, &ctx.host).await?;
            let stream = TcpStream::connect(addr).await?;

            // Apply socket-level configuration here, inside `establish`, so it is
            // re-applied on every reconnect. `StubbornIo` warns that configuration
            // applied externally via `Deref` is silently lost on reconnect, which
            // previously left reconnected sockets with no keepalive at all.
            let ka = socket2::TcpKeepalive::new()
                .with_time(KEEPALIVE_TIME)
                .with_interval(KEEPALIVE_INTERVAL);
            socket2::SockRef::from(&stream).set_tcp_keepalive(&ka)?;

            Ok(Self(stream))
        })
    }

    /// Mirror tokio's `TcpStream` classification (`UnexpectedEof` is not a TCP
    /// disconnect — EOF surfaces as a 0-byte read handled by
    /// [`UnderlyingIo::is_final_read`]).
    fn is_disconnect_error(&self, err: &io::Error) -> bool {
        matches!(
            err.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::NotConnected
                | io::ErrorKind::AddrInUse
                | io::ErrorKind::AddrNotAvailable
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::TimedOut
                | io::ErrorKind::HostUnreachable
                | io::ErrorKind::NetworkUnreachable
                | io::ErrorKind::NetworkDown
        )
    }
}

// `TcpStream` is `Unpin`, so we can project through the newtype without any
// `pin_project` machinery.
impl AsyncRead for CachedDnsTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for CachedDnsTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Regression test: keepalive must be applied inside `establish` so it
    /// survives reconnects. Previously keepalive was set on the `Deref`'d
    /// `TcpStream` after `connect_with_options` returned, which `StubbornIo`
    /// silently discards on every reconnect — and the sender path never set it
    /// at all. This binds a local listener, runs `establish` directly, and
    /// asserts the freshly-built stream has `SO_KEEPALIVE` enabled.
    #[tokio::test]
    async fn establish_enables_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Accept and hold the peer so the connection stays up for inspection.
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let target = ConnectTarget {
            host: Arc::from(format!("127.0.0.1:{port}").as_str()),
            resolver: dns::new_shared_resolver(),
        };

        let stream = CachedDnsTcp::establish(target).await.unwrap();

        let keepalive = socket2::SockRef::from(&stream.0).keepalive().unwrap();
        assert!(
            keepalive,
            "establish must enable SO_KEEPALIVE so it survives reconnects"
        );

        let _peer = accept.await.unwrap();
    }
}
