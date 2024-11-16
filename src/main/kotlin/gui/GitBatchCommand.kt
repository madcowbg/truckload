package gui

import java.io.Closeable
import java.io.File
import java.io.InputStreamReader
import java.io.OutputStreamWriter

open class GitBatchCommand(protected val repoRoot: File, vararg args: String) : Closeable {
    private val builder = ProcessBuilder("git", *args)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .directory(repoRoot)

    init {
        verboseOut.println("Batch copy CMD: ${builder.command()}")
    }

    private val process = builder.start()
    private val commandStream = OutputStreamWriter(process.outputStream)
    private val resultStream = InputStreamReader(process.inputStream).buffered()

    override fun close() {
        process.destroy()
        process.waitFor()
    }

    fun <T> runOnce(batchUnitCmd: String, deserializer: (String) -> T): T {
        verboseOut.println("Issuing batch cmd [$batchUnitCmd]...")

        commandStream.write(batchUnitCmd + "\n")
        commandStream.flush()

        verboseOut.println("Waiting for results...")
        val resultStr = resultStream.readLine()
        verboseOut.println("Result: $resultStr")

        return deserializer(resultStr)
    }
}