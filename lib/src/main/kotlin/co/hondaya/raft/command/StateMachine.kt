package co.hondaya.raft.command

/** Application state machine driven by committed Raft log entries. */
interface StateMachine<C : Any, R : Any> {
    /** Applies a committed command and returns the command result. */
    suspend fun apply(command: C): R
}
