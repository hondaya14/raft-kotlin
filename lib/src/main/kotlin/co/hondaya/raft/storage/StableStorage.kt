package co.hondaya.raft.storage

import co.hondaya.raft.log.LogEntry
import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.Term
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

data class PersistentState<C : Any>(
    val currentTerm: Term = Term(0),
    val votedFor: NodeId? = null,
    val log: List<LogEntry<C>> = emptyList(),
)

interface StableStorage<C : Any> {
    suspend fun load(): PersistentState<C>
    suspend fun saveTermAndVote(currentTerm: Term, votedFor: NodeId?)
    suspend fun appendEntries(entries: List<LogEntry<C>>)
    suspend fun truncateSuffix(fromIndex: LogIndex)
    suspend fun replaceLog(entries: List<LogEntry<C>>)
}

class InMemoryStableStorage<C : Any>(
    initialState: PersistentState<C> = PersistentState(),
) : StableStorage<C> {
    private val mutex = Mutex()
    private var currentTerm = initialState.currentTerm
    private var votedFor = initialState.votedFor
    private val log = initialState.log.sortedBy { it.index.value }.toMutableList()

    override suspend fun load(): PersistentState<C> = mutex.withLock {
        PersistentState(currentTerm, votedFor, log.toList())
    }

    override suspend fun saveTermAndVote(currentTerm: Term, votedFor: NodeId?) {
        mutex.withLock {
            this.currentTerm = currentTerm
            this.votedFor = votedFor
        }
    }

    override suspend fun appendEntries(entries: List<LogEntry<C>>) {
        if (entries.isEmpty()) return
        mutex.withLock {
            entries.forEach { entry ->
                require(entry.index.value == log.size.toLong() + 1) {
                    "entry index ${entry.index.value} does not append after log size ${log.size}"
                }
                log += entry
            }
        }
    }

    override suspend fun truncateSuffix(fromIndex: LogIndex) {
        mutex.withLock {
            require(fromIndex.value >= 1) { "cannot truncate virtual log entry" }
            while (log.isNotEmpty() && log.last().index >= fromIndex) {
                log.removeAt(log.lastIndex)
            }
        }
    }

    override suspend fun replaceLog(entries: List<LogEntry<C>>) {
        mutex.withLock {
            log.clear()
            log += entries.sortedBy { it.index.value }
        }
    }
}

