use crossfire::{mpmc, MAsyncRx};
use log::{debug, error, info};
use std::sync::Arc;
use vetis::log::LogMessage;
use vetis::VetisResult;

pub struct MetricsWorker {
    receiver: Arc<MAsyncRx<mpmc::Array<LogMessage>>>,
}

impl MetricsWorker {
    pub fn new(receiver: MAsyncRx<mpmc::Array<LogMessage>>) -> MetricsWorker {
        MetricsWorker { receiver: receiver.into() }
    }

    pub async fn run(&self) -> VetisResult<()> {
        while let Ok(message) = self
            .receiver
            .recv()
            .await
        {
            // TODO: Define tasks to perform
        }

        Ok(())
    }
}
