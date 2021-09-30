use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::Sender;

use crate::error::Result;
use crate::store::StoreCommand::{Get, Set};

pub type Db = HashMap<String, String>;
pub type Responder<T> = oneshot::Sender<Result<T>>;

#[derive(Debug)]
pub enum StoreCommand {
    Get {
        key: String,
        resp: Responder<Option<String>>,
    },
    Set {
        key: String,
        val: String,
        resp: Responder<()>,
    }
}

pub struct Store {
    tx: Sender<StoreCommand>,
}

impl Store {

    async fn run() -> Store {
        let mut db: Db = HashMap::new();
        let (tx, mut rx) = mpsc::channel::<StoreCommand>(1);

        // use an actor to enforce thread-safe writes/reads on the store's backing hashmap
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Get { key, resp } => {
                        let val = db.get(&key).map(|s| s.clone());
                        let _ = resp.send(Ok(val));
                    },
                    Set { key, val, resp } => {
                        db.insert(key, val);
                        let _ = resp.send(Ok(()));
                    }
                }
            }
        });

        Store { tx }
    }


    async fn set(&mut self, key: String, val: String) -> Result<()> {
        let (res_tx, res_rx) = oneshot::channel();
        self.tx.send(StoreCommand::Set{ key, val, resp: res_tx }).await?;
        res_rx.await?
    }

    async fn get(&mut self, key: String) -> Result<Option<String>> {
        let (res_tx, res_rx) = oneshot::channel();
        self.tx.send(StoreCommand::Get{ key, resp: res_tx }).await?;
        res_rx.await?
    }

}

#[cfg(test)]
mod store_tests {
    use super::*;

    #[tokio::test(flavor ="multi_thread")]
    async fn set_and_get() {
        let mut store = Store::run().await;
        store.set("foo".to_string(), "bar".to_string()).await.unwrap();
        assert_eq!(
            store.get("foo".to_string()).await.unwrap(),
            Some("bar".to_string()),
        )
    }
}
