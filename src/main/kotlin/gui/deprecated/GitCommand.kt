package gui.deprecated

import gui.GitCommandHistory
import gui.GitCommandState
import java.io.Closeable
import java.io.File
import java.io.InputStreamReader

open class GitCommand(repoRoot: File, vararg args: String) : Closeable {
    val builder = ProcessBuilder("git", *args)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .directory(repoRoot)

    init {
        GitCommandHistory.record(builder)
        GitCommandHistory.changeState(builder, GitCommandState.RUNNING)
    }

    val process: Process = builder.start()

    val resultStream = InputStreamReader(process.inputStream).buffered()

    override fun close() {
        process.destroy() // commands need exiting before closing
        GitCommandHistory.changeState(builder, GitCommandState.FINISHED)
        process.waitFor()
    }
}