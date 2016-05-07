gstore-wrapper
===

This repository is written for [gstore](https://github.com/Caesar11/gStore). Gstore System(also called gStore) is a graph database engine for managing large graph-structured data, which is open-source and targets at Linux operation systems.

`GstoreConnector` is a Python API for connecting gstore server, which has been merged into its offical [repository](https://github.com/Caesar11/gStore/tree/master/api/python). `GstoreWrapper` is written for making gstore support operations with uncertain predicates(i.e. `select ?s ?p ?o where {?s ?p ?o.}`). Note that gstore is based on the idea of VStree presented in the gStore-VLDB paper, which improve the efficiency while neglecting uncertain predicates.

License
---

MIT
