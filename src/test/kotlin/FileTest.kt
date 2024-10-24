@file:OptIn(ExperimentalEncodingApi::class)

import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.io.path.fileSize
import kotlin.test.Test

fun Repo.dumpToConsole() {
    println("Dumping repo...")
    files.forEach { version ->
        println("${Base64.encode(version.hash)} ${version.location} ${version.path}")
    }
}

class FileTest {

    @Test
    fun `traverse project folder`() {
        val md5 = MessageDigest.getInstance("md5")
        val repo = Repo(File("./").walk().filter { it.isFile }.mapNotNull {
            val digest = try {
                md5.digest(it.readBytes())
            } catch (e: IOException) {
                return@mapNotNull null
            }

            StoredFileVersion(
                size= it.toPath().fileSize(),
                hash = digest,
                path = it.path,
                location = Location.LocalFilesystem
            )
        }.toList())

        repo.dumpToConsole()

    }
}