package co.hondaya.raft.command

/**
 * Converts commands between the application type and the bytes stored in Raft log entries.
 */
interface CommandCodec<C : Any> {
    /** Encodes an application command for replication and durable storage. */
    fun encode(command: C): ByteArray

    /** Decodes a command received from replicated log bytes. */
    fun decode(bytes: ByteArray): C
}
