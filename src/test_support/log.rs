use crate::error::Result;
use crate::state::log::{Log, LogEntry};
use crate::NEWLINE;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

impl Log {
    pub async fn from_entries(path: String, entries: Vec<LogEntry>) -> Result<Log> {
        let log = Log { path, entries };
        let _ = log.dump().await?;
        Ok(log)
    }

    pub async fn dump(&self) -> Result<()> {
        let entries = self.entries.clone();
        let file = File::create(&self.path).await?;

        let mut writer = BufWriter::new(file);
        for entry in entries {
            writer.write(&entry.to_bytes()).await?;
            writer.write(&[NEWLINE]).await?;
            writer.flush().await?;
        }

        Ok(())
    }
}
