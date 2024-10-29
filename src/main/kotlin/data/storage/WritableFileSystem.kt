package data.storage

interface WritableFileSystem: ReadonlyFileSystem {
    fun copy(file: ReadonlyFileSystem.File, toPath: String)
}