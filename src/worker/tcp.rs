use crate::host::Host;
use hyper::server::conn::http1;
#[cfg(feature = "http2")]
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use log::{debug, error};
use peekable::tokio::AsyncPeekable;
use std::borrow::Cow;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use vetis::{errors::VetisError, server::http::HttpService, VetisHosts, VetisResult};

pub struct TcpWorker {
    acceptor: TlsAcceptor,
    hosts: VetisHosts<Host>,
    allow_plain_connections: bool,
    token: CancellationToken,
}

impl TcpWorker {
    pub fn new(
        acceptor: TlsAcceptor,
        hosts: VetisHosts<Host>,
        allow_plain_connections: bool,
        token: CancellationToken,
    ) -> Self {
        Self { acceptor, hosts, allow_plain_connections, token }
    }

    async fn serve_http1<S>(&mut self, stream: S, service: HttpService<Host>) -> VetisResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let token = self.token.clone();
        tokio::task::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("HTTP/1.1 service stopping...")
                }
                _ = http1::Builder::new()
                    .auto_date_header(true)
                    .serve_connection(TokioIo::new(stream), service) => {}
            };
        })
        .await
        .map_err(|e| VetisError::Worker(e.to_string()))
    }

    #[cfg(feature = "http2")]
    async fn serve_http2<S>(&mut self, stream: S, service: HttpService<Host>) -> VetisResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let token = self.token.clone();
        tokio::task::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("HTTP/2 service stopping...")
                }
                _ =  http2::Builder::new(TokioExecutor::new())
                        .auto_date_header(true)
                        .serve_connection(TokioIo::new(stream), service) => {}
            };
        })
        .await
        .map_err(|e| VetisError::Worker(e.to_string()))
    }

    async fn serve_auto<S>(&mut self, stream: S, service: HttpService<Host>) -> VetisResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let token = self.token.clone();
        tokio::task::spawn(async move {
            let builder = auto::Builder::new(TokioExecutor::new());
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("HTTP auto service stopping...")
                }
                _ = builder.serve_connection_with_upgrades(TokioIo::new(stream), service) => {}
            };
        })
        .await
        .map_err(|e| VetisError::Worker(e.to_string()))
    }

    pub async fn run(&mut self, stream: TcpStream) -> VetisResult<()> {
        let Ok(client_addr) = stream.peer_addr() else {
            return Err(VetisError::Worker("Unkown client address".into()));
        };

        let mut peekable = AsyncPeekable::from(stream);
        let mut peeked = [0; 2];
        peekable
            .peek_exact(&mut peeked)
            .await
            .map_err(|e| VetisError::Worker(e.to_string()))?;

        let service = HttpService::new(self.hosts.clone(), client_addr);
        let is_tls = peeked.starts_with(&[0x16, 0x03]);
        if is_tls {
            let tls_stream = self
                .acceptor
                .accept(peekable)
                .await
                .map_err(|e| VetisError::Worker(e.to_string()))?;

            let alpn = &tls_stream
                .get_ref()
                .1
                .alpn_protocol();
            if let Some(alpn_code) = alpn {
                let Cow::Borrowed(alpn_code) = String::from_utf8_lossy(alpn_code) else {
                    return Err(VetisError::Worker("Cannot accept connection".into()));
                };
                match alpn_code {
                    "http/1.1" => {
                        self.serve_http1(tls_stream, service)
                            .await
                    }
                    #[cfg(feature = "http2")]
                    "h2" => {
                        self.serve_http2(tls_stream, service)
                            .await
                    }
                    _ => {
                        error!("Unsupported protocol: {}", alpn_code);
                        Ok(())
                    }
                }
            } else {
                self.serve_auto(tls_stream, service)
                    .await
            }
        } else {
            if self.allow_plain_connections {
                self.serve_auto(peekable, service)
                    .await
            } else {
                Ok(())
            }
        }
    }
}
