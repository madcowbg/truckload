package gui

import java.io.Closeable
import java.io.File
import java.io.InputStreamReader

open class GitCommand(repoRoot: File, vararg args: String) : Closeable {
    protected val builder = ProcessBuilder("git", *args)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .directory(repoRoot)

    protected val process: Process = builder.start()

    protected val resultStream = InputStreamReader(process.inputStream).buffered()

    override fun close() {
        process.destroy() // commands need exiting before closing
        process.waitFor()
    }
}