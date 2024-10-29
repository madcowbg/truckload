package data.storage

interface WritableFileSystem: ReadonlyFileSystem {
    fun copy(file: ReadonlyFileSystem.File, toPath: String)
    fun write(toPath: String, data: ByteArray)
}