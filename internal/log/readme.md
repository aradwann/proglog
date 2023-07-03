### Log component
- Record - the data stored in our log
- Store - the file we store records in
- Index - the file we store index entries in
- Segment - the abstraction that ties a store and an index together
- Log - the abstraction that ties the segments together

### setup Raft
- A finite state machine that applies the commands you give Raft
- A log store where Raft stores those commands
- A stable store where Raft stores that cluster's configuration- the servers in the cluster, their addresses and so on
- A snapshot store where Raft stores compact snapshots of its data;
- A transport that Raft uses to connect with the servers's peers