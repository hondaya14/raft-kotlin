package co.hondaya.raft.rpc

import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.protocol.toDomain
import co.hondaya.raft.protocol.toProto
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RaftPeerEndpoint
import co.hondaya.raft.transport.RaftService
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.protocol.v1.AppendEntriesRequest as AppendEntriesRequestProto
import co.hondaya.raft.protocol.v1.AppendEntriesResponse as AppendEntriesResponseProto
import co.hondaya.raft.protocol.v1.RaftService as ProtoRaftService
import co.hondaya.raft.protocol.v1.RequestVoteRequest as RequestVoteRequestProto
import co.hondaya.raft.protocol.v1.RequestVoteResponse as RequestVoteResponseProto

class KotlinxRpcRaftEndpointAdapter(
    private val endpoint: RaftPeerEndpoint,
) : ProtoRaftService {
    override suspend fun RequestVote(message: RequestVoteRequestProto): RequestVoteResponseProto =
        endpoint.requestVote(message.toDomain()).toProto()

    override suspend fun AppendEntries(message: AppendEntriesRequestProto): AppendEntriesResponseProto =
        endpoint.appendEntries(message.toDomain()).toProto()
}

class KotlinxRpcRaftServiceAdapter(
    private val peers: Map<NodeId, ProtoRaftService>,
) : RaftService {
    override suspend fun requestVote(
        target: NodeId,
        request: RequestVoteRequest,
    ): RequestVoteResponse {
        val peer = peers[target] ?: return RequestVoteResponse(request.term, voteGranted = false)
        return peer.RequestVote(request.toProto()).toDomain()
    }

    override suspend fun appendEntries(
        target: NodeId,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse {
        val peer = peers[target] ?: return AppendEntriesResponse(request.term, success = false)
        return peer.AppendEntries(request.toProto()).toDomain()
    }
}
