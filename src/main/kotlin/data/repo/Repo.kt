package data.repo

import data.storage.FileSystem
import data.storage.Hash
import data.storage.StoredFileVersion

class RepoFile(val logicalPath: String, val hash: Hash)
class Repo(val files: List<RepoFile>, val storage: List<StoredFileVersion>)

@Deprecated("remove, nothing is actually happening")
fun readFolder(rootLocation: FileSystem): Repo {
    val allFiles = rootLocation.walk().mapNotNull {
        val digest = it.hash ?: return@mapNotNull null // todo better handle of file read errors
        StoredFileVersion(size = it.fileSize, hash = digest, path = it.path, location = rootLocation)
    }.toList()
    return Repo(
        files = allFiles.map { RepoFile(logicalPath = it.path, hash = it.hash) },
        storage = allFiles
    )
}