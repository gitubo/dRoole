Distri-C: Hybrid Consensus Cluster Engine
A high-performance, geographically distributed task execution engine written in Pure C. This system balances strong configuration consistency with elastic, failure-tolerant worker membership using a hybrid Raft/Gossip architecture.

Architectural Overview
Distri-C is designed for scenarios requiring strict auditability of rules but massive scale for execution. It splits the network into two functional planes:

Control Plane (Consistency): A subset of nodes using the Raft Consensus Protocol. This plane serves as the "Source of Truth" for the Key-Value configuration store.

Data Plane (Scalability): A dynamic fleet of worker nodes that receive configuration updates via SWIM-based Gossip and execute tasks on incoming message payloads.

Key Features
Triangular Traffic Flow: Requests enter via Control Nodes but results are returned directly from the executing Worker to the entry node, minimizing internal hops.

Custom Binary Protocol: Zero third-party dependencies for serialization. All communication uses packed C-structs for maximum throughput.

Peer-to-Peer Load Balancing: Nodes forward tasks based on real-time resource metrics (CPU/RAM) disseminated via the Gossip layer.