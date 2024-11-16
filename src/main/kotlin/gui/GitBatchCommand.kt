package gui

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable
import java.io.File
import java.io.OutputStreamWriter

open class GitBatchCommand(val repoRoot: File, vararg args: String) : Closeable {
    internal val cmd = GitCommand(repoRoot, *args)

    private val commandStream = OutputStreamWriter(cmd.process.outputStream)

    override fun close() {
        cmd.close()
    }

    suspend fun <T> runOnce(batchUnitCmd: String, deserializer: (String) -> T): T =
        withContext(Dispatchers.IO) {
            synchronized(cmd.process) {
                verboseOut.println("Issuing batch cmd [$batchUnitCmd]...")

                commandStream.write(batchUnitCmd + "\n")
                commandStream.flush()

                verboseOut.println("Waiting for results...")
                val resultStr = cmd.resultStream.readLine()
                verboseOut.println("Result: $resultStr")

                deserializer(resultStr)
            }
        }
}