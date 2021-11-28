# What lies here

This is a playground for a toy implementation of the Raft algorithm whose purpose is to help me have fun learning about the algorithm and about network programming in Rust.

It is a work in progress that will assemble pieces over time sporadically as I get interested in new things and have time to play!

As a learning project, it will often take long detours into implementing things from scratch that it would make no sense to implement from scratch in a real production environment! In general: it will seek to have good (perhaps even excessive!) test coverage, as testing in Rust is something I am also interested in learning about.


# Progress

- [x] 0. write a server that exposes a TCP + JSON interface (with `get` and `put` commands) to a simple key/value store (implemented as a thread-safe hash-map that can store strings as values)
- [x] 1. write a client for this server
- [x] 2. implement naive (unsafe) replication to get network topology/protocol in place [a]
  - [x] app consists of cluster w/ 1 leader, 4 followers, all of which read and respond to api calls from external clients and rpc calls from other nodes in the cluster.
  - [x] all nodes respond to `get` api call by returning value from hashmap
  - [x] leader responds to `put` api call by issuing `put` rpc call to all followers. if majority respond with success, it writes k/v pair to hashmap, issues success to api caller.
  - [x] followers respond to `put` api call by redirecting to leader.
- [ ] 3. implement proper log replication (w/ commits to state machine)[b]
  - [ ] introduce state machine advanced via application of log entries known to be safely replicated across all clients -- as specified in the Raft Paper (below)
  - [ ] properly test it!
- [ ] 5. refactor api client to know about all servers in cluster (issues `Get` to random node, `Put` to ) -- consider
      migrating api client protocol to http
- [ ] 4. migrate rpc protocol to proper protobuf Rpc over http (using [tonic](https://github.com/hyperium/tonic))
- [ ] 6. implement leader election
- [ ] 7. implement log compaction
- [ ] 8. implement cluster expansion/contraction

[a] this implementation is deliberately unsafe in that it does not provide any way of enforcing an eventually consistent state between leaders and all followers or recovering from any node crashing. the point is simply to sketch out the network topology and protocol for *attempting* to sync state across may nodes in a cluster

[b] we deliberately descope leader election at this stage. the point is to get log replication working

# Resources

# on raft:
- paper: https://raft.github.io/raft.pdf
- slide deck: http://thesecretlivesofdata.com/raft/
- demo & resources: https://raft.github.io/

# on distributed systems
- [Designing Data Intensive Applications](https://github.com/Yang-Yanxiang/Designing-Data-Intensive-Applications/blob/master/Designing%20Data%20Intensive%20Applications.pdf)
