package gui

import java.io.Closeable
import java.io.File
import java.io.OutputStreamWriter

open class GitBatchCommand(protected val repoRoot: File, vararg args: String) : Closeable, GitCommand(repoRoot, *args) {
    private val commandStream = OutputStreamWriter(process.outputStream)

    override fun close() {
        super.close()
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