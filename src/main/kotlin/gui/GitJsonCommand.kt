package gui

import kotlinx.serialization.DeserializationStrategy
import java.io.Closeable
import java.io.File

abstract class GitPerLineOutputCommand<T>(repoRoot: File, vararg args: String) : Closeable,
    GitCommand(repoRoot, *args) {

    abstract val results: CompletableSequence<T>

    override fun close() {
        results.makeEager() // forces reading the remaining data
        super.close()
    }
}

class GitJsonCommand<T>(repoRoot: File, private val deserializer: DeserializationStrategy<T>, vararg args: String) :
    GitPerLineOutputCommand<T>(repoRoot, *args) {

    override val results: CompletableSequence<T> = resultStream.lineSequence().map { line ->
        jsonDecoder.decodeFromString(deserializer, line)
    }.completable()
}