package gui.deprecated

import gui.GitCommandHistory
import gui.GitCommandState
import gui.jsonDecoder
import kotlinx.coroutines.*
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.Serializable
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader

object Git {
    suspend fun <T> executeOnAnnex(
        root: File,
        strategy: DeserializationStrategy<T>,
        vararg args: String
    ): T? {
        val processBuilder = ProcessBuilder("git", "annex", *args)
            .directory(root)
        GitCommandHistory.record(processBuilder)
        println(processBuilder.command())
        val process = withContext(Dispatchers.IO) {
            GitCommandHistory.changeState(processBuilder, GitCommandState.RUNNING)

            val process = processBuilder.start()
            process.waitFor()

            GitCommandHistory.changeState(processBuilder, GitCommandState.FINISHED)
            process
        }

        return toJsonIfSuccessfulAndNonempty(process, strategy)
    }
}

private fun <T> toJsonIfSuccessfulAndNonempty(process: Process, strategy: DeserializationStrategy<T>): T? {
    return if (process.exitValue() != 0) {
        println(BufferedReader(InputStreamReader(process.errorStream)).readText())
        null
    } else {
        val result = BufferedReader(InputStreamReader(process.inputStream)).readText()
        if (result.isEmpty()) {
            println("Process returned no result")
            null
        } else {
            jsonDecoder.decodeFromString(strategy, result)
        }
    }
}
