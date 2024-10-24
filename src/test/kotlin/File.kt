
interface Location {
    object LocalFilesystem: Location
}

class StoredFileVersion(val path: String, val location: Location, val hash: ByteArray, val size: Long)

class Repo(val files: List<StoredFileVersion>)