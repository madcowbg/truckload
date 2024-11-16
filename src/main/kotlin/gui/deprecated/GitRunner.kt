package gui.deprecated

import gui.GitCommandHistory
import gui.GitCommandState
import gui.fromJsonIfSuccessfulAndNonempty
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.DeserializationStrategy
import java.io.File

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

        return strategy.fromJsonIfSuccessfulAndNonempty().invoke(process)
    }
}

