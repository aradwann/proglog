# Distributed Commit Log service
![Go workflow](https://github.com/aradwann/proglog/actions/workflows/go.yml/badge.svg)

### internal log package
- Record - the data stored in our log
- Store - the file we store records in
- Index - the file we store index entries in
- Segment - the abstraction that ties a store and an index together
- Log - the abstraction that ties the segments together