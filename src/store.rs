use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Error = Box<dyn std::error::Error>;
// pub type Result<T> = std::result::Result<T, Error>;

pub type Db = Arc<Mutex<HashMap<String, String>>>;


pub struct Store {
    db: Db
}

impl Store {
    fn new() -> Store {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set(&mut self, key: &str, val: &str) -> Result<(), Error> {
        let mut db = self.db.lock().expect("Failed to acquire lock.");
        db.insert(key.to_string(), val.to_string()).ok_or("Write failed")?;
        Ok(())

    }

    fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        let db = self.db.lock().expect("Failed to acquire lock.");
        Ok(db.get(key).map(|s| s.clone()))
    }

}

#[cfg(test)]
mod store_tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let mut store = Store::new();
        store.set("foo", "bar").unwrap();
        assert_eq!(
            store.get("foo").unwrap(),
            Some("bar".to_string()),
        )
    }
}
