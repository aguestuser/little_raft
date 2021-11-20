use serde::{Deserialize, Serialize};
use serde_json;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

use crate::error::PersistenceError::{LogDeserializationError, RemoveFromEmptyLogError};
use crate::{AsyncError, Result, NEWLINE};

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
pub struct LogEntry {
    pub(crate) term: usize,
    pub(crate) command: Command,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Command {
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
    fn to_bytes(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl Into<String> for LogEntry {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Log {
    pub async fn load(path: &str) -> Result<Log> {
        let file = File::open(path).await?;
        let entries = LinesStream::new(BufReader::new(file).lines())
            .filter_map(|line| line.ok())
            .filter_map(|ok_line| LogEntry::from(ok_line).ok())
            .collect::<Vec<LogEntry>>()
            .await;
        Ok(Log {
            entries,
            path: path.to_string(),
        })
    }

    pub async fn append(&mut self, entry: LogEntry) -> Result<()> {
        let file = OpenOptions::new().append(true).open(&self.path).await?;

        let mut writer = BufWriter::new(file);
        writer.write(&entry.clone().to_bytes()).await?;
        writer.write(&[NEWLINE]).await?;
        writer.flush().await?;

        self.entries.push(entry);
        Ok(())
    }

    pub async fn append_many(&mut self, entries: Vec<LogEntry>) -> Result<()> {
        for entry in entries {
            self.append(entry).await?;
        }
        Ok(())
    }

    pub async fn remove(&mut self) -> Result<LogEntry> {
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
            self.remove().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test_log {
    use super::*;

    // TODO: i've been avoiding figuring out teardown logic (b/c rust tests don't seem to support
    //  them very easily), but would be nice to do that here as it would enable tests to run in
    //  parallel by not always clobbering the same log file in `data/test_log_x.txt`.

    impl Log {
        pub async fn new(path: String, entries: Vec<LogEntry>) -> Result<Log> {
            let log = Log { path, entries };
            let _ = log.dump().await?;
            Ok(log)
        }

        pub async fn dump(&self) -> Result<()> {
            let entries = self.entries.clone();
            // let mut tmp_path = self.path.clone();
            // tmp_path.push_str(".tmp");
            let file = File::create(&self.path).await?;

            let mut writer = BufWriter::new(file);
            for entry in entries {
                writer.write(&entry.to_bytes()).await?;
                writer.write(&[NEWLINE]).await?;
                writer.flush().await?;
            }
            // fs::rename(tmp_path, &self.path).await?;

            Ok(())
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

    #[tokio::test]
    async fn loads_log_from_file() {
        let path = "data/test_log_0.txt".to_string();
        let log = Log::load(&path).await.unwrap();
        assert_eq!(log.path, path,);
        assert_eq!(
            log.entries,
            vec![
                LogEntry {
                    term: 1,
                    command: Command::Put {
                        key: "foo".to_string(),
                        value: "bar".to_string(),
                    }
                },
                LogEntry {
                    term: 2,
                    command: Command::Put {
                        key: "foo".to_string(),
                        value: "baz".to_string(),
                    }
                },
                LogEntry {
                    term: 3,
                    command: Command::Put {
                        key: "foo".to_string(),
                        value: "qux".to_string(),
                    }
                },
            ]
        );
    }

    #[tokio::test]
    async fn appends_entry_to_log() {
        let path = "data/test_log_1.txt";
        let original_entries = vec![LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        }];
        let entry_to_append = LogEntry {
            term: 2,
            command: Command::Put {
                key: "foo".to_string(),
                value: "baz".to_string(),
            },
        };
        let expected_final_entries: Vec<LogEntry> =
            [original_entries.clone(), vec![entry_to_append.clone()]].concat();

        let mut log = Log::new(String::from(path), original_entries)
            .await
            .unwrap();
        let _ = log.append(entry_to_append).await.unwrap();
        let persisted_log = Log::load(path).await.unwrap();

        assert_eq!(log.entries, expected_final_entries);
        assert_eq!(persisted_log.entries, expected_final_entries);
    }

    #[tokio::test]
    async fn removes_entry_from_log() {
        let path = "data/test_log_2.txt";
        let original_entries = vec![
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
        ];
        let expected_final_entries: Vec<LogEntry> = vec![LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        }];

        let mut log = Log::new(String::from(path), original_entries)
            .await
            .unwrap();
        let _ = log.remove().await.unwrap();
        let persisted_log = Log::load(path).await.unwrap();

        assert_eq!(log.entries, expected_final_entries);
        assert_eq!(persisted_log.entries, expected_final_entries);
    }

    #[tokio::test]
    async fn appends_many_entries_to_log() {
        let path = "data/test_log_3.txt";
        let original_entries = vec![LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        }];
        let entries_to_append = vec![
            LogEntry {
                term: 2,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "baz".to_string(),
                },
            },
            LogEntry {
                term: 2,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "qux".to_string(),
                },
            },
        ];
        let expected_final_entries = [original_entries.clone(), entries_to_append.clone()].concat();

        let mut log = Log::new(String::from(path), original_entries)
            .await
            .unwrap();
        let _ = log.append_many(entries_to_append).await.unwrap();
        let persisted_log = Log::load(path).await.unwrap();

        assert_eq!(log.entries, expected_final_entries);
        assert_eq!(persisted_log.entries, expected_final_entries);
    }

    #[tokio::test]
    async fn removes_many_entries_from_log() {
        let path = "data/test_log_4.txt";
        let original_entries = vec![
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
        ];
        let expected_final_entries: Vec<LogEntry> = vec![LogEntry {
            term: 1,
            command: Command::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        }];

        let mut log = Log::new(String::from(path), original_entries)
            .await
            .unwrap();
        let _ = log.remove_many(2).await.unwrap();
        let persisted_log = Log::load(path).await.unwrap();

        assert_eq!(log.entries, expected_final_entries);
        assert_eq!(persisted_log.entries, expected_final_entries);
    }
}
