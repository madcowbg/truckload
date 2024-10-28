package data.storage

import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KCallable

interface FileSystem {
    fun resolve(path: String): File
    fun walk(): Sequence<File>

    interface File {
        val location: FileSystem
        val hash: Hash
        val path: String
        val fileSize: Long
        fun dataInRange(from: Long, to: Long): ByteArray
    }
}
