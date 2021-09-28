# What lies here

This is a playground for toy projects implementing distributed data stores in rust in different ways.

It is a work in progress that will assemble pieces over time sporadically as I get interested in new things and have time to play!

Currently I am interested in implementing the various examples of approaches to replication outlined in Chapter 5 of Martin Kleppman's *Designing Data-Intensive Applications*. 

# Current state

- [ ] 0: write a server that exposes an HTTPS + JSON interface (with `get` and `set` commands) to a simple key/value store (implemented as a thread-safe hash-map that can store strings or ints as values)
- [ ] 1: write a client for this server
- [ ] 2: extend the server into a 6-server cluster (1 load balancer, 1 leader, 1 synchronous follower, 3 async followers)
  - [ ] 2.0: routes all writes to a hard-coded leader
  - [ ] 2.1: replicates writes to all followers and returns error to client if sync replication fails, else success
  - [ ] 2.2: distributes reads across followers (by picking random follower on every request)

# Up next:
- handle adding new follower (leader takes snapshot, appends changes to log, follower updates from snapshot + logs)
- elect new leader if leader fails
- use some more interesting protocol than HTTP + JSON
- persisist leader store to disk with write-through semantics
- use a more realistic "log"
- ...

# Sources

# on distributed systems
- [Designing Data Intensive Applications](https://github.com/Yang-Yanxiang/Designing-Data-Intensive-Applications/blob/master/Designing%20Data%20Intensive%20Applications.pdf)

# on raft:
- https://raft.github.io/
- http://thesecretlivesofdata.com/raft/

# on paxos:
- https://people.cs.rutgers.edu/~pxk/417/notes/paxos.html
- https://www.microsoft.com/en-us/research/publication/paxos-made-simple/
