package co.hondaya.raft.node

import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.log.Term

/**
 * Point-in-time status snapshot for a Raft node.
 *
 * @property id node identity.
 * @property state current Raft role.
 * @property currentTerm latest term observed by this node.
 * @property votedFor candidate that received this node's vote in [currentTerm], if any.
 * @property leaderId latest known leader, or `null` when unknown.
 * @property commitIndex highest log index known to be committed.
 * @property lastApplied highest committed log index applied to the state machine.
 * @property lastLogIndex highest log index stored locally.
 * @property lastLogTerm term of [lastLogIndex].
 */
data class RaftStatus(
    val id: NodeId,
    val state: NodeState,
    val currentTerm: Term,
    val votedFor: NodeId?,
    val leaderId: NodeId?,
    val commitIndex: LogIndex,
    val lastApplied: LogIndex,
    val lastLogIndex: LogIndex,
    val lastLogTerm: Term,
)
