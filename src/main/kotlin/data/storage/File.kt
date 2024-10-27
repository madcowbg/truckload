package data.storage

import java.io.File

interface Location {
    fun resolve(path: String): File

    class LocalFilesystem(val repoLocation: String) : Location {
        override fun resolve(path: String): File = File(repoLocation).resolve(path)
    }
}

class StoredFileVersion(
    val path: String,
    val location: Location,
    val hash: Hash,
    val size: Int // FIXME make long to support files >2GB
) {
    init {
        check(!File(path).isAbsolute) { "Path $path is not absolute" }
    }

    val fileObject: File = location.resolve(path)
}

