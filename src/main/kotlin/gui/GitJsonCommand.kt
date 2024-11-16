package gui

import kotlinx.serialization.DeserializationStrategy
import java.io.Closeable
import java.io.File
import java.io.InputStreamReader

abstract class GitPerLineOutputCommand<T>(repoRoot: File, vararg args: String) : Closeable {
    private val process = ProcessBuilder("git", *args)
        .directory(repoRoot)
        .start()

    protected val resultStream = InputStreamReader(process.inputStream).buffered()

    abstract val results: CompletableSequence<T>

    override fun close() {
        results.close()
        process.waitFor()
    }
}

class GitJsonCommand<T>(repoRoot: File, private val deserializer: DeserializationStrategy<T>, vararg args: String) :
    GitPerLineOutputCommand<T>(repoRoot, *args) {

    override val results: CompletableSequence<T> = resultStream.lineSequence().map { line ->
        jsonDecoder.decodeFromString(deserializer, line)
    }.completable()
}