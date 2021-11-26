use serde::{Deserialize, Serialize};
use serde_json;
use tokio::fs::metadata;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, ErrorKind};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

use crate::error::PersistenceError::{LogDeserializationError, RemoveFromEmptyLogError};
use crate::error::{AsyncError, Result};
use crate::state::log::Command::NoOp;
use crate::NEWLINE;

lazy_static! {
    static ref INIT_LOG_LINE: Vec<u8> = [
        LogEntry {
            term: 0,
            command: NoOp,
        }
        .to_bytes(),
        vec![NEWLINE]
    ]
    .concat();
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
pub struct LogEntry {
    pub term: usize,
    pub command: Command,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Command {
    NoOp,
    Put { key: String, value: String },
}

pub struct Log {
    pub path: String,
    pub entries: Vec<LogEntry>,
}

impl LogEntry {
    fn from(line: String) -> Result<LogEntry> {
        serde_json::from_str(&line)
            .map_err(|e| Box::new(LogDeserializationError(e.to_string())) as AsyncError)
    }
    fn to_string(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    pub(crate) fn to_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl Into<String> for LogEntry {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Log {
    pub fn new(path: String) -> Log {
        Log {
            path,
            entries: Vec::new(),
        }
    }

    /// Attempt to load log entries from a specified `path` and construct a `Log` with the path
    /// and retrieved entries on success. If we find an empty file at the `path`, append a NoOp
    /// command to the log to produce a strong guarantee that all logs have at least one entry.
    /// This ensures we never have to reason about "previous indexes" with negative values
    /// (and thus pollute the entire codebase with Option<usize> to prevent integer overflows that
    /// are only ever a threat on the first time we append an entry).
    pub async fn load_from(path: &str) -> Result<Log> {
        let _ = Self::initialize_if_empty(path).await?;

        let entries = LinesStream::new(BufReader::new(File::open(path).await?).lines())
            .filter_map(|line| line.ok())
            .filter_map(|ok_line| LogEntry::from(ok_line).ok())
            .collect::<Vec<LogEntry>>()
            .await;

        Ok(Log {
            entries,
            path: path.to_string(),
        })
    }

    pub async fn initialize_if_empty(path: &str) -> Result<()> {
        match metadata(&path).await {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let _ = File::create(path).await?.write_all(&INIT_LOG_LINE).await?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub async fn append(&mut self, entry: &LogEntry) -> Result<()> {
        let file = OpenOptions::new().append(true).open(&self.path).await?;

        let mut writer = BufWriter::new(file);
        writer.write(&entry.clone().to_bytes()).await?;
        writer.write(&[NEWLINE]).await?;
        writer.flush().await?;

        self.entries.push(entry.clone());
        Ok(())
    }

    pub async fn append_many(&mut self, entries: &Vec<LogEntry>) -> Result<()> {
        for entry in entries {
            self.append(entry).await?;
        }
        Ok(())
    }

    pub async fn remove_last(&mut self) -> Result<LogEntry> {
        let file = OpenOptions::new().write(true).open(&self.path).await?;
        let err = || Box::new(RemoveFromEmptyLogError) as AsyncError;

        let last_entry = self.entries.last().ok_or(err())?.clone();
        let file_len = file.metadata().await?.len();
        let bytes_to_truncate = last_entry.to_bytes().len() as u64 + 1; // +1 for \n
        let _ = file.set_len(file_len - bytes_to_truncate).await?;

        let entry = self.entries.pop().ok_or(err())?;
        Ok(entry)
    }

    pub async fn remove_many(&mut self, n: usize) -> Result<()> {
        for _ in 0..n {
            self.remove_last().await?;
        }
        Ok(())
    }

    pub fn has_matching(&self, index: usize, term: usize) -> bool {
        self.entries
            .get(index)
            .map_or(false, |entry| entry.term == term)
    }

    pub fn find_conflict(
        &self,
        new_entries: &Vec<LogEntry>,
        first_idx_to_compare: usize,
    ) -> Option<usize> {
        for (i, new_entry) in new_entries.iter().enumerate() {
            let idx_to_compare = first_idx_to_compare + i;
            let has_conflict = self
                .entries
                .get(idx_to_compare)
                .map_or(false, |entry| entry.term != new_entry.term);
            if has_conflict {
                return Some(idx_to_compare);
            }
        }
        return None;
    }

    /// Remove backwards from tail of the log until reaching the entry at `idx`, which will now be
    /// the last entry in the truncated log.
    pub async fn remove_until(&mut self, idx: usize) -> Result<()> {
        self.remove_many(self.entries.len() - idx - 1).await
    }

    /// Retrieve the index of the last entry in a log. We do not check against overflow
    /// subtraction (which would occur in the case we were trying to find the index of the last
    /// entry in an empty log of length 0) b/c we insert a NoOp command into an empty log to
    /// guarantee all logs have at least length 1.
    pub fn get_last_index(&self) -> usize {
        self.entries.len() - 1
    }

    /// Retrieve the term of a log entry at the given index. Panic if we try to retrieve
    /// the term of an index not in the log. (Safe to do b/c it is a programming error if that
    /// ever happens.)
    pub fn get_term_at(&self, index: usize) -> usize {
        assert!(self.entries.len() > index);
        self.entries[index].term
    }
}

#[cfg(test)]
mod test_log {
    use test_context::{test_context, AsyncTestContext};

    use crate::test_support::gen::Gen;

    use super::*;

    lazy_static! {
        static ref NOOP_ENTRY: LogEntry = LogEntry {
            term: 0,
            command: Command::NoOp,
        };
        static ref PUT_ENTRY: LogEntry = LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        static ref LOG_ENTRIES: Vec<LogEntry> = vec![
            LogEntry {
                term: 0,
                command: Command::NoOp,
            },
            LogEntry {
                term: 1,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                },
            },
            LogEntry {
                term: 2,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "baz".to_string(),
                },
            },
            LogEntry {
                term: 3,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "qux".to_string(),
                },
            },
            LogEntry {
                term: 4,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "qux".to_string(),
                },
            },
        ];
    }

    struct Context {
        log_path: String,
        original_entries: Vec<LogEntry>,
    }

    impl Context {
        async fn setup(original_entries: Vec<LogEntry>) -> Self {
            let log_path = format!("test_data/log_{}.txt", Gen::usize().to_string());
            if !original_entries.is_empty() {
                let _ = Log::from_entries(log_path.clone(), original_entries.clone())
                    .await
                    .unwrap();
            }
            Self {
                log_path,
                original_entries,
            }
        }
        async fn teardown(self) {
            let _ = tokio::fs::remove_file(self.log_path).await.unwrap();
        }
    }

    struct EmptyLog(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for EmptyLog {
        async fn setup() -> Self {
            let ctx = Context::setup(vec![]).await;
            EmptyLog(ctx)
        }

        async fn teardown(self) {
            self.0.teardown().await;
        }
    }

    struct LogWithEntries(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for LogWithEntries {
        async fn setup() -> Self {
            let ctx = Context::setup(LOG_ENTRIES.clone()).await;
            LogWithEntries(ctx)
        }

        async fn teardown(self) {
            self.0.teardown().await;
        }
    }

    #[test]
    fn serializes_a_log_entry() {
        let entry = LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        let expected_result = r#"{"term":1,"command":{"type":"Put","key":"foo","value":"bar"}}"#;
        let actual_result: String = entry.into();
        assert_eq!(expected_result, actual_result);
    }

    #[test]
    fn deserializes_a_log_entry() {
        let serialized_entry =
            r#"{"term":1,"command":{"type":"Put","key":"foo","value":"bar"}}"#.to_string();
        let expected_result = LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        let actual_result = LogEntry::from(serialized_entry).unwrap();
        assert_eq!(expected_result, actual_result);
    }

    #[test_context(EmptyLog)]
    #[tokio::test]
    async fn loads_an_empty_log_and_inserts_noop_entry(ctx: EmptyLog) {
        let log = Log::load_from(&ctx.0.log_path).await.unwrap();
        assert_eq!(log.path, ctx.0.log_path);
        assert_eq!(log.entries, vec![NOOP_ENTRY.clone()])
    }

    #[test_context(LogWithEntries)]
    #[tokio::test]
    async fn loads_log_from_file(ctx: LogWithEntries) {
        let log = Log::load_from(&ctx.0.log_path).await.unwrap();
        assert_eq!(log.path, ctx.0.log_path);
        assert_eq!(log.entries, LOG_ENTRIES.clone());
    }

    #[test_context(EmptyLog)]
    #[tokio::test]
    async fn appends_entry_to_log(ctx: EmptyLog) {
        let mut log = Log::load_from(&ctx.0.log_path).await.unwrap();
        let _ = log.append(&PUT_ENTRY).await.unwrap();
        let persisted_log = Log::load_from(&log.path).await.unwrap();

        assert_eq!(log.entries, vec![NOOP_ENTRY.clone(), PUT_ENTRY.clone()]);
        assert_eq!(
            persisted_log.entries,
            vec![NOOP_ENTRY.clone(), PUT_ENTRY.clone()]
        );
    }

    #[test_context(LogWithEntries)]
    #[tokio::test]
    async fn removes_entry_from_log(ctx: LogWithEntries) {
        let mut log = Log::load_from(&ctx.0.log_path).await.unwrap();
        let _ = log.remove_last().await.unwrap();
        let persisted_log = Log::load_from(&ctx.0.log_path).await.unwrap();

        assert_eq!(
            &log.entries,
            &ctx.0.original_entries[0..ctx.0.original_entries.len() - 1]
        );
        assert_eq!(
            persisted_log.entries,
            &ctx.0.original_entries[0..ctx.0.original_entries.len() - 1]
        );
    }

    #[test_context(EmptyLog)]
    #[tokio::test]
    async fn appends_many_entries_to_log(ctx: EmptyLog) {
        let mut log = Log::load_from(&ctx.0.log_path).await.unwrap();
        let _ = log
            .append_many(&vec![PUT_ENTRY.clone(), PUT_ENTRY.clone()])
            .await
            .unwrap();
        let persisted_log = Log::load_from(&ctx.0.log_path).await.unwrap();

        assert_eq!(
            log.entries,
            vec![NOOP_ENTRY.clone(), PUT_ENTRY.clone(), PUT_ENTRY.clone()]
        );
        assert_eq!(
            persisted_log.entries,
            vec![NOOP_ENTRY.clone(), PUT_ENTRY.clone(), PUT_ENTRY.clone()]
        );
    }

    #[test_context(LogWithEntries)]
    #[tokio::test]
    async fn removes_many_entries_from_log(ctx: LogWithEntries) {
        let original_entries = ctx.0.original_entries.clone();
        let mut log = Log::load_from(&ctx.0.log_path).await.unwrap();
        let _ = log.remove_many(2).await.unwrap();
        let persisted_log = Log::load_from(&ctx.0.log_path).await.unwrap();

        assert_eq!(
            &log.entries,
            &original_entries[0..original_entries.len() - 2]
        );
        assert_eq!(
            &persisted_log.entries,
            &original_entries[0..original_entries.len() - 2]
        );
    }

    #[test_context(LogWithEntries)]
    #[tokio::test]
    async fn removes_all_entries_in_log_until_a_given_index(ctx: LogWithEntries) {
        let LogWithEntries(Context {
            log_path,
            original_entries,
        }) = &ctx;

        let mut log = Log::load_from(&log_path).await.unwrap();
        let _ = log.remove_until(1).await.unwrap();
        let persisted_log = Log::load_from(&log_path).await.unwrap();

        assert_eq!(log.entries, original_entries[0..2]);
        assert_eq!(persisted_log.entries, original_entries[0..2]);
    }
}
