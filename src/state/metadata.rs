use crate::error::PersistenceError::MetadataParseError;
use crate::error::Result;

use tokio::fs::metadata;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};

const CURRENT_TERM_FILEPATH: &'static str = "/current_term.txt";

pub struct PersistentMetadata {
    current_term_path: String,
    pub current_term: usize,
}

impl PersistentMetadata {
    /// Load persistent metadata from a given metadata directory, creating files for storing
    /// each metadata item and initializing them if none already exist.
    pub async fn load_from(path: String) -> Result<PersistentMetadata> {
        let current_term_path: String = path.clone() + CURRENT_TERM_FILEPATH;
        let _ = Self::initialize_if_empty(&current_term_path).await?;
        let current_term = Self::read_usize(&current_term_path).await?;

        Ok(Self {
            current_term_path,
            current_term,
        })
    }

    #[allow(unused)]
    pub async fn update_current_term(&mut self, term: usize) -> Result<()> {
        let _ = File::create(&self.current_term_path)
            .await?
            .write_all(term.to_string().as_bytes())
            .await?;
        self.current_term = term;
        Ok(())
    }

    async fn read_usize(path: &str) -> Result<usize> {
        atoi::atoi::<usize>(&Self::read_value(&path).await?).ok_or(MetadataParseError.boxed())
    }

    pub async fn read_value(path: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::<u8>::new();
        let _ = File::open(path).await?.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    pub async fn initialize_if_empty(path: &str) -> Result<()> {
        match metadata(&path).await {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let _ = File::create(path).await?.write_all("0".as_bytes()).await?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
