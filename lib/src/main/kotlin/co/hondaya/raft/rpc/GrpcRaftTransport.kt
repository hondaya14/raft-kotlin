@file:OptIn(ExperimentalRpcApi::class, InternalRpcApi::class)

package co.hondaya.raft.rpc

import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.protocol.toDomain
import co.hondaya.raft.protocol.toProto
import co.hondaya.raft.protocol.v1.AppendEntriesRequest
import co.hondaya.raft.protocol.v1.AppendEntriesRequestInternal
import co.hondaya.raft.protocol.v1.AppendEntriesResponse
import co.hondaya.raft.protocol.v1.AppendEntriesResponseInternal
import co.hondaya.raft.protocol.v1.RequestVoteRequest
import co.hondaya.raft.protocol.v1.RequestVoteRequestInternal
import co.hondaya.raft.protocol.v1.RequestVoteResponse
import co.hondaya.raft.protocol.v1.RequestVoteResponseInternal
import co.hondaya.raft.transport.RaftPeerEndpoint
import co.hondaya.raft.transport.RaftService
import io.grpc.CallOptions
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.MethodDescriptor
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.ClientCalls
import io.grpc.stub.ServerCalls
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.rpc.grpc.descriptor.GrpcMethodType
import kotlinx.rpc.grpc.descriptor.methodDescriptor
import kotlinx.rpc.internal.utils.ExperimentalRpcApi
import kotlinx.rpc.internal.utils.InternalRpcApi
import java.io.Closeable
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import co.hondaya.raft.transport.AppendEntriesRequest as DomainAppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse as DomainAppendEntriesResponse
import co.hondaya.raft.transport.RequestVoteRequest as DomainRequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse as DomainRequestVoteResponse

data class GrpcPeerAddress(
    val host: String,
    val port: Int,
) {
    init {
        require(host.isNotBlank()) { "host must not be blank" }
        require(port in 1..65535) { "port must be between 1 and 65535" }
    }
}

class GrpcRaftService(
    peerAddresses: Map<NodeId, GrpcPeerAddress>,
) : RaftService, Closeable {
    private val channels: Map<NodeId, ManagedChannel> = peerAddresses.mapValues { (_, address) ->
        ManagedChannelBuilder
            .forAddress(address.host, address.port)
            .usePlaintext()
            .build()
    }

    override suspend fun requestVote(
        target: NodeId,
        request: DomainRequestVoteRequest,
    ): DomainRequestVoteResponse {
        val channel = channels[target] ?: return DomainRequestVoteResponse(request.term, voteGranted = false)
        return try {
            runUnaryRpc(channel, REQUEST_VOTE_METHOD, request.toProto()).toDomain()
        } catch (_: StatusRuntimeException) {
            DomainRequestVoteResponse(request.term, voteGranted = false)
        }
    }

    override suspend fun appendEntries(
        target: NodeId,
        request: DomainAppendEntriesRequest,
    ): DomainAppendEntriesResponse {
        val channel = channels[target] ?: return DomainAppendEntriesResponse(request.term, success = false)
        return try {
            runUnaryRpc(channel, APPEND_ENTRIES_METHOD, request.toProto()).toDomain()
        } catch (_: StatusRuntimeException) {
            DomainAppendEntriesResponse(request.term, success = false)
        }
    }

    override fun close() {
        channels.values.forEach { it.shutdown() }
        channels.values.forEach { channel ->
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow()
            }
        }
    }

    private suspend fun <ReqT, RespT> runUnaryRpc(
        channel: ManagedChannel,
        method: MethodDescriptor<ReqT, RespT>,
        request: ReqT,
    ): RespT = withContext(Dispatchers.IO) {
        ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT, request)
    }
}

class GrpcRaftServer(
    private val port: Int,
    endpoint: RaftPeerEndpoint,
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : Closeable {
    private val scope = CoroutineScope(SupervisorJob() + coroutineContext)
    private val server: Server = ServerBuilder
        .forPort(port)
        .addService(raftServerServiceDefinition(endpoint, scope))
        .build()

    val boundPort: Int
        get() = server.port

    fun start(): GrpcRaftServer {
        server.start()
        return this
    }

    fun awaitTermination() {
        server.awaitTermination()
    }

    override fun close() {
        server.shutdown()
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow()
        }
        scope.cancel()
    }
}

private const val SERVICE_NAME = "raft.v1.RaftService"

private val REQUEST_VOTE_METHOD: MethodDescriptor<RequestVoteRequest, RequestVoteResponse> =
    methodDescriptor(
        fullMethodName = MethodDescriptor.generateFullMethodName(SERVICE_NAME, "RequestVote"),
        requestMarshaller = RequestVoteRequestInternal.MARSHALLER,
        responseMarshaller = RequestVoteResponseInternal.MARSHALLER,
        type = GrpcMethodType.UNARY,
        schemaDescriptor = null,
        idempotent = false,
        safe = false,
        sampledToLocalTracing = true,
    )

private val APPEND_ENTRIES_METHOD: MethodDescriptor<AppendEntriesRequest, AppendEntriesResponse> =
    methodDescriptor(
        fullMethodName = MethodDescriptor.generateFullMethodName(SERVICE_NAME, "AppendEntries"),
        requestMarshaller = AppendEntriesRequestInternal.MARSHALLER,
        responseMarshaller = AppendEntriesResponseInternal.MARSHALLER,
        type = GrpcMethodType.UNARY,
        schemaDescriptor = null,
        idempotent = false,
        safe = false,
        sampledToLocalTracing = true,
    )

private fun raftServerServiceDefinition(
    endpoint: RaftPeerEndpoint,
    scope: CoroutineScope,
): ServerServiceDefinition =
    ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
            REQUEST_VOTE_METHOD,
            ServerCalls.asyncUnaryCall { request, response ->
                scope.respond(response) {
                    endpoint.requestVote(request.toDomain()).toProto()
                }
            },
        )
        .addMethod(
            APPEND_ENTRIES_METHOD,
            ServerCalls.asyncUnaryCall { request, response ->
                scope.respond(response) {
                    endpoint.appendEntries(request.toDomain()).toProto()
                }
            },
        )
        .build()

private fun <T> CoroutineScope.respond(
    response: StreamObserver<T>,
    block: suspend () -> T,
) {
    launch {
        try {
            response.onNext(block())
            response.onCompleted()
        } catch (error: Throwable) {
            response.onError(
                Status.UNKNOWN
                    .withDescription(error.message ?: error::class.simpleName)
                    .withCause(error)
                    .asRuntimeException(),
            )
        }
    }
}
