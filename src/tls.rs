use crate::host::Host;
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::ResolvesServerCertUsingSni,
    sign::CertifiedKey,
};
use std::{collections::HashSet, sync::Arc};
use vetis::{
    VetisHosts,
    errors::{StartError, VetisError},
    security::Alpn,
};

pub(crate) struct TlsFactory {}

impl TlsFactory {
    pub(crate) async fn create_tls_config(
        hosts: VetisHosts<Host>,
    ) -> Result<Arc<ServerConfig>, VetisError> {
        let hosts = hosts.clone();
        #[cfg(feature = "__rustls_awc_lc_rs")]
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        #[cfg(feature = "__rustls_ring")]
        let provider = rustls::crypto::ring::default_provider();
        let mut resolver = ResolvesServerCertUsingSni::new();
        let mut alpns: HashSet<Vec<u8>> = HashSet::new();
        for (hostname, host) in hosts
            .pin_owned()
            .iter()
        {
            if let Some(tls) = host.tls() {
                let cert = tls.cert();
                let key = tls.key();

                let cert = CertificateDer::from(cert.to_vec());
                let mut chain = vec![cert];
                if let Some(ca_cert) = tls.ca() {
                    let ca_cert = CertificateDer::from(ca_cert.to_vec());
                    chain.push(ca_cert);
                }

                let key = PrivateKeyDer::try_from(key.to_vec())
                    .map_err(|_| VetisError::Tls("Failed to parse private key".to_string()))?;
                let certified_key = CertifiedKey::from_der(chain, key, &provider).map_err(|e| {
                    VetisError::Tls(format!("Failed to create certified key: {}", e))
                })?;

                let (hostname, _) = hostname
                    .rsplit_once(':')
                    .unwrap_or_else(|| (hostname, "443"));

                alpns.extend(
                    tls.supported_alpns()
                        .iter()
                        .map(From::<&Alpn>::from),
                );

                resolver
                    .add(&hostname, certified_key)
                    .map_err(|e| VetisError::Tls(e.to_string()))?;
            }
        }

        let builder = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(rustls::ALL_VERSIONS)
            .map_err(|e| VetisError::Start(StartError::Tls(e.to_string())))?;

        // TODO: Add client verification (mTLS)

        let mut tls_config = builder
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));
        tls_config.max_early_data_size = u32::MAX;
        tls_config.alpn_protocols = Vec::from_iter(alpns);
        Ok(tls_config.into())
    }
}
