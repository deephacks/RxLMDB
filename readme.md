# RxLMDB

RxLMDB provide a [RxJava](https://github.com/ReactiveX/RxJava) API to [LMDB](http://symas.com/mdb/) which is an ultra-fast, ultra-compact key-value embedded data store developed by Symas for the OpenLDAP Project. LMDB uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases. Transactional with full ACID semantics and crash-proof by design. No corruption. No startup time. No dependencies.

Adding RxJava to LMDB make it possible for asynchronous and event-based programs to process data from LMDB as sequences and adds operators that allow you to compose sequences together declaratively while abstracting away concerns about things like low-level threading, synchronization, thread-safety and concurrent data structures.
