package data.storage

import data.repo.Repo
import data.repo.RepoFile
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlin.io.path.fileSize

interface Location {
    object LocalFilesystem: Location
}

class StoredFileVersion(val path: String, val location: Location, val hash: ByteArray, val size: Long) {
    init {
        check(!File(path).isAbsolute) {"Path $path is not absolute"}
    }
}


fun readFolder(rootFolder: File): Repo {
    val md5 = MessageDigest.getInstance("md5")
    val allFiles = rootFolder.walk().filter { it.isFile }.mapNotNull {
        val digest = try {
            md5.digest(it.readBytes())
        } catch (e: IOException) {
            return@mapNotNull null
        }

        StoredFileVersion(
            size = it.toPath().fileSize(),
            hash = digest,
            path = it.relativeTo(rootFolder).path,
            location = Location.LocalFilesystem
        )
    }.toList()
    return Repo(
        files = allFiles.map { RepoFile(logicalPath = it.path, hash = it.hash) },
        storage = allFiles
    )
}