package data.storage

import jdk.internal.util.Preconditions
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import kotlin.io.path.fileSize
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

class DeviceFileSystem(rootFolder: String) : FileSystem {
    private val root: File = File(rootFolder)

    inner class DeviceFile(private val file: File) : FileSystem.File {
        override val hash: Hash?
            get() = try {
                Hash.digest(file.readBytes())
            } catch (e: IOException) {
                null
            }

        override val path: String = file.relativeTo(root).path

        override fun fileSize(): Long = file.toPath().fileSize()
        override fun dataInRange(from: Long, to: Long): ReadOnlyProperty<FileReference, ByteArray> =
            ReadOnlyProperty { thisRef, property ->
                check(from in 0..to) { "invalid range to read data from $file -> $from..$to" }
                val size: Long = to - from
                check(size <= Int.MAX_VALUE) { "can't read $size bytes, max is ${Int.MAX_VALUE}" }
                val data = ByteArray(size.toInt())
                val openedFile = RandomAccessFile(file, "r")
                openedFile.seek(from)
                openedFile.readFully(data)
                return@ReadOnlyProperty data
            }

        override fun hashCode(): Int = file.hashCode()
        override fun equals(other: Any?): Boolean = other is DeviceFile && file == other.file
    }


    override fun resolve(path: String): FileSystem.File {
        check(!File(path).isAbsolute) { "Path $path is not absolute" }
        return DeviceFile(root.resolve(path))
    }

    override fun walk(): Sequence<FileSystem.File> =
        root.walk().filter { it.isFile }.map { resolve(it.relativeTo(root).path) }
}