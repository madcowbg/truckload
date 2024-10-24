@file:OptIn(ExperimentalEncodingApi::class)

package data

import data.repo.Repo
import data.repo.RepoFile
import data.storage.Location
import data.storage.StoredFileVersion
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.io.path.fileSize
import kotlin.test.Test

fun Repo.dumpToConsole() {
    println("DUMPING REPO")
    println("Files:")
    files.forEach { println("[${it.logicalPath}] ${Base64.encode(it.hash)}") }
    println("Storage:")
    storage.forEach { version -> println("${Base64.encode(version.hash)} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}

fun readFolder(path: File): Repo {
    val md5 = MessageDigest.getInstance("md5")
    val allFiles = path.walk().filter { it.isFile }.mapNotNull {
        val digest = try {
            md5.digest(it.readBytes())
        } catch (e: IOException) {
            return@mapNotNull null
        }

        StoredFileVersion(
            size = it.toPath().fileSize(),
            hash = digest,
            path = it.path,
            location = Location.LocalFilesystem
        )
    }.toList()
    return Repo(
        files = allFiles.map { RepoFile(logicalPath = it.path, hash = it.hash) },
        storage = allFiles
    )
}

class FileTest {

    @Test
    fun `traverse project folder`() {
        val repo: Repo = readFolder(File("./"))

        repo.dumpToConsole()
    }
}