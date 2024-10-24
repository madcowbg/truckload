
interface Location {
    object LocalFilesystem: Location
}

class StoredFileVersion(val path: String, val location: Location, val hash: ByteArray, val size: Long)

class RepoFile(val logicalPath: String, val hash: ByteArray)
class Repo(val files: List<RepoFile>, val storage: List<StoredFileVersion>)