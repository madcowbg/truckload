package data.storage

import kotlin.properties.ReadOnlyProperty

interface FileSystem {
    fun resolve(path: String): File
    fun walk(): Sequence<File>

    interface File {
        val hash: Hash?

        val path: String

        fun fileSize(): Long
        fun dataInRange(from: Long, to: Long): ReadOnlyProperty<FileReference, ByteArray>
    }
}
