package data.repo

import data.storage.Location
import data.storage.StoredFileVersion
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlin.io.path.fileSize

class RepoFile(val logicalPath: String, val hash: ByteArray)
class Repo(val files: List<RepoFile>, val storage: List<StoredFileVersion>)


fun readFolder(rootFolder: File): Repo {
    val rootLocation = Location.LocalFilesystem(rootFolder.path)
    val md5 = MessageDigest.getInstance("md5")
    val allFiles = rootFolder.walk().filter { it.isFile }.mapNotNull {
        val digest = try {
            md5.digest(it.readBytes())
        } catch (e: IOException) {
            return@mapNotNull null
        }

        StoredFileVersion(
            size = it.toPath().fileSize().toInt(), // FIXME 2GB files only?
            hash = digest,
            path = it.relativeTo(rootFolder).path,
            location = rootLocation
        )
    }.toList()
    return Repo(
        files = allFiles.map { RepoFile(logicalPath = it.path, hash = it.hash) },
        storage = allFiles
    )
}