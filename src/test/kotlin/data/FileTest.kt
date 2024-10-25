@file:OptIn(ExperimentalEncodingApi::class)

package data

                                                         import data.repo.Repo
import data.storage.readFolder
import java.io.File
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.test.Test

fun Repo.dumpToConsole() {
    println("DUMPING REPO")
    println("Files:")
    files.forEach { println("[${it.logicalPath}] ${Base64.encode(it.hash)}") }
    println("Storage:")
    storage.forEach { version -> println("${Base64.encode(version.hash)} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}


class FileTest {

    @Test
    fun `traverse project folder`() {
        val repo: Repo = readFolder(File("./.experiments/data"))

        repo.dumpToConsole()
    }
}