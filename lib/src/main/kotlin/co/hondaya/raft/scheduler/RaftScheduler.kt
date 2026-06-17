package co.hondaya.raft.scheduler

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

interface RaftScheduler {
    suspend fun delay(duration: Duration)
    fun nextElectionTimeout(): Duration
}

class DefaultRaftScheduler(
    private val random: Random = Random.Default,
    private val electionTimeoutMin: Duration = 150.milliseconds,
    private val electionTimeoutMax: Duration = 300.milliseconds,
) : RaftScheduler {
    init {
        require(electionTimeoutMin.isPositive()) { "minimum election timeout must be positive" }
        require(electionTimeoutMax >= electionTimeoutMin) {
            "maximum election timeout must be at least minimum election timeout"
        }
    }

    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration {
        val minMs = electionTimeoutMin.inWholeMilliseconds
        val maxMs = electionTimeoutMax.inWholeMilliseconds
        return random.nextLong(minMs, maxMs + 1).milliseconds
    }
}

