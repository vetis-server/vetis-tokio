mod handler;

use crate::host::path::HandlerPath;
use vetis::errors::{HandlerError, HostError, VetisError};

#[test]
fn test_handler_path_builder_validation() {
    let empty_uri = HandlerPath::builder()
        .uri("")
        .build();
    assert!(matches!(empty_uri, Err(VetisError::Host(HostError::Handler(HandlerError::Uri(_))))));

    let missing_handler = HandlerPath::builder().build();
    assert!(matches!(
        missing_handler,
        Err(VetisError::Host(HostError::Handler(HandlerError::Handler(_))))
    ));
}
