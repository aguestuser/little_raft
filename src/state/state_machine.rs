use crate::state::log::{Command, LogEntry};
use crate::state::store::Store;
use std::sync::Arc;

pub struct StateMachine {
    store: Arc<Store>,
}

impl StateMachine {
    pub fn new(store: Arc<Store>) -> StateMachine {
        StateMachine { store }
    }

    pub async fn apply(&self, entry: LogEntry) {
        match entry.command {
            Command::Put { key, value } => self.store.put(&key, &value).await,
        };
    }

    pub async fn apply_many(&self, entries: Vec<LogEntry>) {
        for entry in entries {
            self.apply(entry).await;
        }
    }
}

#[cfg(test)]
mod test_state_machine {
    use super::*;

    lazy_static! {
        static ref ENTRIES: Vec<LogEntry> = vec![
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
                    key: "bar".to_string(),
                    value: "qux".to_string(),
                }
            },
        ];
    }

    #[tokio::test]
    async fn applies_log_entries_to_a_store() {
        let store = Arc::new(Store::new());
        let state_machine = StateMachine::new(store.clone());
        let _ = state_machine.apply_many(ENTRIES.clone()).await;
        assert_eq!(store.get("foo").await, Some("baz".to_string()));
        assert_eq!(store.get("bar").await, Some("qux".to_string()));
    }
}
