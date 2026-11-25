use crate::host::Host;
use compact_str::ToCompactString;
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use vetis::{
    Str, VetisHosts,
    errors::{StartupError, VetisError},
    security::Alpn,
};

pub(crate) struct TlsConfigResolver {
    pub(crate) hosts_tls_config: HashMap<Str, Arc<ServerConfig>>,
}

impl TlsConfigResolver {
    pub(crate) fn create_tls_config(hosts: VetisHosts<Host>) -> Result<Self, VetisError> {
        let hosts = hosts.clone();
        let mut sni_tls_config_map = HashMap::new();
        for (hostname, host) in hosts
            .pin_owned()
            .iter()
        {
            if let Some(tls) = host.tls() {
                let mut alpns: HashSet<Vec<u8>> = HashSet::new();

                #[cfg(feature = "__rustls_awc_lc_rs")]
                let provider = rustls::crypto::aws_lc_rs::default_provider();
                #[cfg(feature = "__rustls_ring")]
                let provider = rustls::crypto::ring::default_provider();

                let builder = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
                    .with_protocol_versions(rustls::ALL_VERSIONS)
                    .map_err(|e| VetisError::Startup(StartupError::Tls(e.to_compact_string())))?;

                let cert = tls.cert();
                let key = tls.key();

                let cert = CertificateDer::from(cert.to_vec());
                let mut chain = vec![cert];
                if let Some(ca_cert) = tls.ca() {
                    let ca_cert = CertificateDer::from(ca_cert.to_vec());
                    chain.push(ca_cert);
                }

                let key = PrivateKeyDer::try_from(key.to_vec())
                    .map_err(|_| VetisError::tls("Failed to parse private key"))?;

                let (hostname, _) = hostname
                    .rsplit_once(':')
                    .unwrap_or_else(|| (hostname, "443"));

                alpns.extend(
                    tls.supported_alpns()
                        .iter()
                        .map(From::<&Alpn>::from),
                );

                // TODO: Add client verification (mTLS)

                let mut tls_config = builder
                    .with_no_client_auth()
                    .with_single_cert(chain, key)
                    .map_err(|e| VetisError::Tls(e.to_compact_string()))?;

                tls_config.max_early_data_size = 0;
                tls_config.alpn_protocols = Vec::from_iter(alpns);

                sni_tls_config_map.insert(hostname.to_compact_string(), tls_config.into());
            }
        }

        Ok(Self { hosts_tls_config: sni_tls_config_map })
    }
}
