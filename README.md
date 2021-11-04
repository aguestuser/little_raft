# What lies here

This is a playground for a toy implementation of the Raft algorithm whose purpose is to help me have fun learning about the algorithm and about network programming in Rust.

It is a work in progress that will assemble pieces over time sporadically as I get interested in new things and have time to play!

As a learning project, it will often take long detours into implementing things from scratch that it would make no sense to implement from scratch in a real production environment! In general: it will seek to have good (perhaps even excessive!) test coverage, as testing in Rust is something I am also interested in learning about.


# Progress

- [x] 0. write a server that exposes a TCP + JSON interface (with `get` and `set` commands) to a simple key/value store (implemented as a thread-safe hash-map that can store strings as values)
- [x] 1. write a client for this server
- [ ] 2. implement naive (unsafe) replication
  - [ ] app consistes of 5 node cluster w/ 1 leader, 4 followers. followers only have servers, leaders have server and client that can issue commands to all followers.
  - [ ] followers respond to `set` command by writing to local hashmap, sending success message
  - [ ] leaders respond to `set` sommand by issuing `set` command to followers. if majority respond with success, it writes k/v pair to hashmap, issues success to caller
  - [x] NOTE: this is unsafe (and not very useful in establishing any meaningful kind of consensus). there is no way of enforcing an eventually consistent state between leaders and all followers or recovering from any node crashing. the point is simply to get the basic topolgy of multi-node communication in place!
- [ ] 3. migrate protocol to protobuf rpc (using [tonic](https://github.com/hyperium/tonic)).
  - [x] NOTE: we do this before implementing log replication in 4. b/c that implementation will cause the  surface area & complexity of of the RPC API to expand dramatically and we wish to migrate to our desired framework while it is still relatively small (and so we don't implement this API twice!)
- [ ] 4. implement proper log replication (w/ commits to state machine)
  - [ ] each node keeps an append-only log of `set` commands (implemented as a `Vec<Commmand::Set>` that serializes its elements and appends them to a file before appending them?) and an append-only log of commit indexes (to track which elements of the log have been committed)
  - [ ] leaders issue `ApendEntry` RPC calls as specified in the Raft Paper (below)
  - [x] NOTE: we descope leader election at this stage!
ly want before doing so involves a large migration
- [ ] 5. implement heartbeats
- [ ] 6. implement leader election
- [ ] 7. implement log compaction
- [ ] 8. implement cluster expansion/contraction

# Resources

# on raft:
- paper: https://raft.github.io/raft.pdf
- slide deck: http://thesecretlivesofdata.com/raft/
- demo & resources: https://raft.github.io/

# on distributed systems
- [Designing Data Intensive Applications](https://github.com/Yang-Yanxiang/Designing-Data-Intensive-Applications/blob/master/Designing%20Data%20Intensive%20Applications.pdf)
