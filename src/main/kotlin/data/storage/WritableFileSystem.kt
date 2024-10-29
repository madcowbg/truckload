package data.storage

interface WritableFileSystem {
    fun copy(file: ReadonlyFileSystem.File, toPath: String)
}