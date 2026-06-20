package co.hondaya.raft.demo

import co.hondaya.raft.cluster.ClusterConfig
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.command.CommandCodec
import co.hondaya.raft.command.StateMachine
import co.hondaya.raft.loop.CoroutineRaftNode
import co.hondaya.raft.rpc.GrpcPeerAddress
import co.hondaya.raft.rpc.GrpcRaftServer
import co.hondaya.raft.rpc.GrpcRaftService
import co.hondaya.raft.storage.InMemoryStableStorage
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.nio.charset.StandardCharsets
import kotlin.time.Duration.Companion.milliseconds

fun main() = runBlocking {
    val selfId = NodeId(requiredEnv("RAFT_NODE_ID"))
    val port = requiredEnv("RAFT_PORT").toInt()
    val peers = parsePeers(env("RAFT_PEERS").orEmpty())
    val heartbeatInterval = env("RAFT_HEARTBEAT_MS")?.toLongOrNull()?.milliseconds ?: 75.milliseconds

    val raftService = GrpcRaftService(peers)
    val node = CoroutineRaftNode(
        config = ClusterConfig(
            selfId = selfId,
            peers = peers.keys,
            heartbeatInterval = heartbeatInterval,
        ),
        storage = InMemoryStableStorage(),
        service = raftService,
        codec = StringCommandCodec,
        stateMachine = LoggingStringStateMachine(selfId),
    )
    val server = GrpcRaftServer(port, node).start()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            runBlocking {
                node.stop()
                server.close()
                raftService.close()
            }
        },
    )

    node.start()
    println("raft node ${selfId.value} listening on $port with peers ${peers.toLogString()}")

    while (true) {
        delay(1_000)
        val status = node.status()
        println(
            "status node=${status.id.value} state=${status.state} term=${status.currentTerm.value} " +
                "leader=${status.leaderId?.value ?: "-"} commit=${status.commitIndex.value} " +
                "applied=${status.lastApplied.value} lastLog=${status.lastLogIndex.value}",
        )
    }
}

private object StringCommandCodec : CommandCodec<String> {
    override fun encode(command: String): ByteArray = command.toByteArray(StandardCharsets.UTF_8)

    override fun decode(bytes: ByteArray): String = bytes.toString(StandardCharsets.UTF_8)
}

private class LoggingStringStateMachine(
    private val nodeId: NodeId,
) : StateMachine<String, String> {
    override suspend fun apply(command: String): String {
        println("applied node=${nodeId.value} command=$command")
        return command
    }
}

private fun parsePeers(value: String): Map<NodeId, GrpcPeerAddress> {
    if (value.isBlank()) return emptyMap()
    return value.split(",").associate { peer ->
        val parts = peer.split("=", limit = 2)
        require(parts.size == 2) { "peer must be formatted as nodeId=host:port: $peer" }
        val endpoint = parts[1].split(":", limit = 2)
        require(endpoint.size == 2) { "peer endpoint must be formatted as host:port: ${parts[1]}" }
        NodeId(parts[0]) to GrpcPeerAddress(endpoint[0], endpoint[1].toInt())
    }
}

private fun Map<NodeId, GrpcPeerAddress>.toLogString(): String =
    entries.joinToString(prefix = "[", postfix = "]") { (id, address) ->
        "${id.value}=${address.host}:${address.port}"
    }

private fun requiredEnv(name: String): String =
    requireNotNull(env(name)) { "$name is required" }

private fun env(name: String): String? = System.getenv(name)?.takeIf { it.isNotBlank() }
