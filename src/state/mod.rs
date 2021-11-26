use crate::error::ProtocolError::RetryAppendEntry;
use crate::error::Result;
use crate::rpc::request::{AppendEntriesRequest, RpcRequest};
use crate::rpc::response::AppendEntriesResponse;
use crate::state::log::{Command, Log, LogEntry};
use crate::state::machine::StateMachine;
use crate::state::metadata::PersistentMetadata;
use crate::state::store::Store;
use crate::NodeAddr;

use dashmap::DashMap;

use std::cmp::min;

use std::sync::Arc;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::{Mutex, MutexGuard};

pub mod log;
pub mod machine;
pub mod metadata;
pub mod store;

pub struct StateConfig {
    pub leader_address: NodeAddr,
    pub node_address: NodeAddr,
    pub peer_addresses: Vec<NodeAddr>,
    pub log_path: String,
    pub metadata_path: String,
}

pub struct State {
    pub leader_metadata: Mutex<LeaderMetadata>,
    pub node_metadata: Mutex<NodeMetadata>,
    pub peer_metadata: PeerMetadata,
    pub log: Mutex<Log>,
    pub store: Arc<Store>,
    pub state_machine: Mutex<StateMachine>,
    pub on_apply_callbacks: Arc<DashMap<usize, OneShotSender<()>>>,
}

pub struct LeaderMetadata {
    address: NodeAddr,
}

pub struct NodeMetadata {
    pub address: String,           // node's (serialized) address
    persisted: PersistentMetadata, // persisently stored values for current term & voted for
    pub last_commit: usize,        // index of last committed log entry
    pub last_applied: usize,       // index of last log entry applied to state machine
}

pub struct PeerMetadata {
    // index of next log entry to send to each per (initialized to leader last log index + 1)
    next_indexes_by_peer: DashMap<String, usize>,
    // index of highest log entry known to be replicated on each peer (initialized to 0, increases monotonically)
    match_indexes_by_peer: DashMap<String, usize>,
}

impl LeaderMetadata {
    fn new(address: NodeAddr) -> LeaderMetadata {
        Self { address }
    }
}

impl NodeMetadata {
    fn new(address: NodeAddr, persisted: PersistentMetadata) -> NodeMetadata {
        Self {
            address,
            persisted,
            last_commit: 0,
            last_applied: 0,
        }
    }

    fn current_term(&self) -> usize {
        self.persisted.current_term
    }
}

impl PeerMetadata {
    fn new(peer_addresses: Vec<NodeAddr>, next_index: usize) -> PeerMetadata {
        Self {
            next_indexes_by_peer: DashMap::from(
                peer_addresses
                    .clone()
                    .into_iter()
                    .map(|addr| (addr, next_index))
                    .collect(),
            ),
            match_indexes_by_peer: DashMap::from(
                peer_addresses.into_iter().map(|addr| (addr, 0)).collect(),
            ),
        }
    }
}

impl StateConfig {
    /// Create a live `State` wrapper from an inert `StateConfig` by loading the log and persistent
    /// metadata_for_test_node about node states from disk, initializing volatile node metadata_for_test_node, and initializing
    /// the store, state machine, and callback registry to notify subscribers when log entries
    /// have been applied to the state machine.
    pub async fn run(self) -> Result<State> {
        let log = Log::load_from(&self.log_path).await?;
        let persisted = PersistentMetadata::load_from(self.metadata_path).await?;
        let store = Arc::new(Store::new());

        Ok(State {
            leader_metadata: Mutex::new(LeaderMetadata::new(self.leader_address)),
            node_metadata: Mutex::new(NodeMetadata::new(self.node_address, persisted)),
            peer_metadata: PeerMetadata::new(self.peer_addresses, log.len()),
            log: Mutex::new(log),
            state_machine: Mutex::new(StateMachine::new(store.clone())),
            store,
            on_apply_callbacks: Arc::new(DashMap::new()),
        })
    }
}

impl State {
    /// Attempt to fetch a `key`'s corresponding value from the `Store`. (Permits dirty reads)
    pub async fn fetch_from_store(&self, key: &str) -> Option<String> {
        self.store.get(key).await
    }

    /// Append a `Command` to the `Log`, return the log's new length
    pub async fn append_to_log(&self, command: Command) -> Result<usize> {
        let mut log = self.log.lock().await;
        let node = self.node_metadata.lock().await;
        let entry = LogEntry {
            term: node.persisted.current_term,
            command,
        };
        let _ = log.append(&entry).await?;
        Ok(log.entries.len() - 1)
    }

    /// Retrieve the index of the last entry in the `Log`
    pub async fn get_last_appended_index(&self) -> usize {
        let log = self.log.lock().await;
        log.entries.len() - 1
    }

    /// Retrieve the socket address of the current leader (as updated in `handle_append_entry_request`)
    pub async fn get_leader_address(&self) -> NodeAddr {
        let leader = self.leader_metadata.lock().await;
        leader.address.clone()
    }

    /// Register a callback to be notified when a `LogEntry` with index `log_index` has been
    /// applied to the `StateMachine`.
    pub fn register_on_apply_handler(&self, log_index: usize, handler: OneShotSender<()>) {
        self.on_apply_callbacks.insert(log_index, handler);
    }

    /// (LEADERS ONLY)
    /// Generate Rpc calls needed to replicate unsynced log entries to all followers, as follows:
    /// For every follower, determine which log entries have been appended to the leader's log but
    /// not the follower's by comparing follower's `next_index` with leader's `last_appended_index`).
    /// Include all such entries in an `AppendEntryRequest` for the given follower, and return this
    /// struct in a tuple with the follower's `PeerAddr` to allow the caller to route requests to
    /// the appropriate follower.
    pub async fn gen_append_entry_requests(&self) -> Vec<(NodeAddr, RpcRequest)> {
        let node = self.node_metadata.lock().await;
        let log = self.log.lock().await;
        let last_leader_index = log.get_last_index();

        self.peer_metadata
            .next_indexes_by_peer
            .iter()
            .map(|key_value| {
                let (peer_address, &next_peer_index) = key_value.pair();
                let request = AppendEntriesRequest {
                    entries: log.entries[next_peer_index..=last_leader_index].to_vec(),
                    leader_address: node.address.clone(),
                    leader_commit: node.last_commit,
                    leader_term: node.persisted.current_term,
                    prev_log_index: next_peer_index - 1,
                    prev_log_term: log.get_term_at(next_peer_index - 1),
                };
                (peer_address.clone(), RpcRequest::AppendEntries(request))
            })
            .collect()
    }

    /// (FOLLOWERS ONLY)
    /// Handle an `AppendEntryRequest` from a leader node, and reply with an
    /// `AppendEntryResponse` with a `success` field that indicates whether all entries were appended
    /// successfully and a `peer_term` field indicating what the follower's current term is.
    ///
    /// Return early without appending anything if:
    ///
    /// 1. follower's current term  < leader's current term (§5.1)
    /// 2. follower's log doesn’t contain an entry at the index just before the entries-to-be-appended
    ///    with a term matching that of of the leader's entry at that index
    /// 3. follower has an existing entry at any index that conflicts with an entry-to-be-appended --
    ///    ie: has same index but different term -- in which case, we delete the conflicting entry and
    ///    all following it (§5.3)
    ///
    /// Otherwise (on happy path):
    ///
    /// 1. append any new entries not already in the log
    /// 2. update the local last commit to the minimum of the leader's last commit or the index
    ///    of the last newly-appended entry (only IFF the leader's last commit is higher than follower's)
    /// 3. apply all log entries up until the local last commit to the state machine
    pub async fn handle_append_entries_request(
        &self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let mut log = self.log.lock().await;
        let mut machine = self.state_machine.lock().await;
        let mut node = self.node_metadata.lock().await;
        let mut leader = self.leader_metadata.lock().await;
        let callbacks = self.on_apply_callbacks.clone();

        // If any steps fail, report back as if we have made no progress (so leader will retry)
        let failure_response = AppendEntriesResponse {
            peer_term: node.current_term(),
            success: false,
        };

        if request.leader_term < node.current_term() {
            return failure_response;
        }
        if !log.has_matching(request.prev_log_index, request.prev_log_term) {
            return failure_response;
        }
        if let Some(conflict_idx) = log.find_conflict(&request.entries, request.prev_log_index + 1)
        {
            let _ = log.remove_until(conflict_idx - 1).await;
            return failure_response;
        }
        if let Err(_) = log.append_many(&request.entries).await {
            // TODO: do we need more checks for "not already in the log"?
            return failure_response;
        }

        if request.leader_commit > node.last_commit {
            // set last commit to lesser of idx of leader's last commit or idx of last-appended entry
            let last_commit = min(
                request.leader_commit,
                request.prev_log_index + request.entries.len(),
            );
            node.last_commit = last_commit;
            // apply all log entries up to this index
            Self::apply_all_until(last_commit, &mut machine, &mut node, &mut log, callbacks).await;
        }

        if request.leader_address != leader.address {
            leader.address = request.leader_address;
        }

        AppendEntriesResponse {
            peer_term: node.persisted.current_term.clone(),
            success: true,
        }
    }
    /// (LEADERS ONLY)
    /// TODO: clean up this doc string!
    /// If successful: update nextIndex and matchIndex for follower (§5.3)
    /// - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
    /// • If there exists an N such that N > commitIndex, a majority of matchIndex\[i\] ≥ N,
    /// and log\[N\].term == currentTerm: set commitIndex = N (§5.3, §5.4).
    /// then check conditions for updating state machine, namely:
    ///  If commitIndex > lastApplied: increment lastApplied, apply log\[lastApplied\] to state machine (§5.3
    pub async fn handle_append_entry_response(
        &self,
        peer_address: NodeAddr,
        req: AppendEntriesRequest,
        resp: AppendEntriesResponse,
    ) -> Result<()> {
        let next_indexes = &self.peer_metadata.next_indexes_by_peer;
        let match_indexes = &self.peer_metadata.match_indexes_by_peer;

        /*** SAD PATH ***/
        if !resp.success {
            // TODO: don't unwrap here...
            let new_next_index = *next_indexes.get(&peer_address).unwrap().value() - 1;
            let _ = next_indexes.insert(peer_address, new_next_index);
            return Err(RetryAppendEntry(new_next_index).boxed());
        }

        /*** HAPPY PATH ***/

        // take locks for all state we are about to mutate
        let log = self.log.lock().await;
        let mut machine = self.state_machine.lock().await;
        let mut node = self.node_metadata.lock().await;
        let callbacks = self.on_apply_callbacks.clone();

        // update metadata_for_test_node to reflect commits made on followers
        let new_match_index = req.prev_log_index + req.entries.len();
        let _ = match_indexes.insert(peer_address.clone(), new_match_index);
        let new_next_index = new_match_index + 1;
        let _ = next_indexes.insert(peer_address, new_next_index);

        // if there is a new index up to which all followers have committed, apply all log entries up to that index
        let curr_match_indexes = match_indexes
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        if let Some(new_consensus_idx) =
            Self::find_new_consensus_idx(curr_match_indexes, &mut node, &log).await
        {
            // println!("> Found new consensus idx: {:?}", new_consensus_idx);
            node.last_commit = new_consensus_idx;
            Self::apply_all_until(new_consensus_idx, &mut machine, &mut node, &log, callbacks)
                .await;
        }

        Ok(())
    }

    /// (ALL NODES)
    /// Apply all log entries up to and including `last_committed`, update node metadata_for_test_node accordingly,
    /// and trigger callbacks registered by `Node::handle_api_requests` (so that node may indicate
    /// success to client that issued the command that has just been applied).
    async fn apply_all_until<'a>(
        last_committed: usize,
        machine: &mut MutexGuard<'a, StateMachine>,
        node: &mut MutexGuard<'a, NodeMetadata>,
        log: &MutexGuard<'a, Log>,
        callbacks: Arc<DashMap<usize, OneShotSender<()>>>,
    ) {
        // println!("> Applying {:?} to {:?}", node.last_applied, last_committed);
        machine
            .apply_many(&log.entries[node.last_applied..=last_committed])
            .await;

        for idx in node.last_applied..=last_committed {
            if let Some((_, cb)) = callbacks.remove(&idx) {
                let _ = cb.send(()); // TODO: handle failure to send callback?
            }
        }

        node.last_applied = last_committed;
    }

    /// (LEADERS ONLY)
    /// Seek backwards from the leader's last log entry to find an index that denoting an entry which:
    ///   1. has not yet been committed on the leader (ie: `index > node.last_commit`)
    ///   2. has already been committed on a majority of followers
    ///   3. belongs to the current term
    /// If such an index is found, store it as the new `last_commit` and return it, else return `None`.
    async fn find_new_consensus_idx<'a>(
        match_indexes: Vec<usize>,
        node: &MutexGuard<'a, NodeMetadata>,
        log: &MutexGuard<'a, Log>,
    ) -> Option<usize> {
        let current_term = node.current_term();
        let num_peers = match_indexes.len();
        let majority = num_peers / 2 + num_peers % 2;

        let first_uncommitted_idx = node.last_commit + 1;
        let last_entry_idx = log.entries.len() - 1;
        for candidate_idx in (first_uncommitted_idx..=last_entry_idx).rev() {
            let num_matches = match_indexes
                .iter()
                .filter(|&&match_idx| match_idx >= candidate_idx)
                .count();
            if num_matches >= majority && log.entries[candidate_idx].term == current_term {
                return Some(candidate_idx);
            }
        }

        return None;
    }
}
