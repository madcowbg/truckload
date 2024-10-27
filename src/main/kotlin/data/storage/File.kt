package data.storage

class StoredFileVersion(
    val path: String,
    val location: FileSystem,
    val hash: Hash,
    val size: Long
) {
    val fileObject: FileSystem.File = location.resolve(path)
}

