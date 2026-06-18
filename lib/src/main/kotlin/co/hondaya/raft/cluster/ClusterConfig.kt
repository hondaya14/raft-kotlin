package co.hondaya.raft.cluster

import kotlin.time.Duration

data class ClusterConfig(
    val selfId: NodeId,
    val peers: Set<NodeId>,
    val heartbeatInterval: Duration,
) {
    init {
        require(selfId !in peers) { "self id must not be present in peers" }
        require(heartbeatInterval.isPositive()) { "heartbeat interval must be positive" }
    }

    val clusterSize: Int = peers.size + 1
    val majority: Int = (clusterSize / 2) + 1
}
