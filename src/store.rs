use dashmap::DashMap;

/// Thin wrapper around a concurrent hashmap. Wrap it in an Arc to share
/// between threads or tasks. (No Mutex needed!)
pub struct Store {
    pub(in crate::store) db: DashMap<String, String>,
}

impl Store {
    pub fn new() -> Store {
        Self { db: DashMap::new() }
    }

    pub async fn set(&self, key: &str, val: &str) -> Option<()> {
        self.db.insert(key.to_string(), val.to_string()).map(|_| ())
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.db.get(&key.to_string()).map(|s| s[..].to_string())
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn set_a_value() {
        let store = Store::new();
        let _ = store.set("foo", "bar").await;

        assert_eq!(&store.db.get("foo").unwrap()[..], "bar");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_an_existing_value() {
        let store = Store::new();
        let _ = store.set("foo", "bar").await;

        assert_eq!(store.get("foo").await, Some("bar".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_a_non_existing_value() {
        let store = Store::new();
        let _ = store.set("foo", "bar").await;

        assert_eq!(store.get("not here").await, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reset_a_value() {
        let store = Store::new();

        let _ = store.set("foo", "bar").await;
        let res1 = store.get("foo").await.unwrap();

        let _ = store.set("foo", "baz").await;
        let res2 = store.get("foo").await.unwrap();

        assert_eq!(res1, "bar".to_string());
        assert_eq!(res2, "baz".to_string());
    }
}
