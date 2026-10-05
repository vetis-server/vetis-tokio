use log::{debug, error, info, trace};
use std::sync::Arc;
use vetis::{LogReceiver, VetisResult};

pub(crate) struct LogWorker {
    receiver: Arc<LogReceiver>,
}

unsafe impl Send for LogWorker {}

impl LogWorker {
    pub(crate) fn new(receiver: LogReceiver) -> LogWorker {
        LogWorker { receiver: receiver.into() }
    }

    pub(crate) fn run(&self) -> VetisResult<()> {
        info!(target: "vetis", "Logger started!");
        while let Ok(message) = self.receiver.recv() {
            match message.level() {
                "INFO" => info!(target: message.target(), "{}", message.message()),
                "DEBUG" => debug!(target: message.target(), "{}", message.message()),
                "ERROR" => error!(target: message.target(), "{}", message.message()),
                "TRACE" => trace!(target: message.target(), "{}", message.message()),
                &_ => unreachable!(),
            }
        }
        info!(target: "vetis", "Logger stopped successfully!");

        Ok(())
    }
}
