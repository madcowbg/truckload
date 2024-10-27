package data.storage

import java.io.File
import java.io.IOException
import kotlin.io.path.fileSize

interface FileSystem {
    fun resolve(path: String): File
    fun walk(): Sequence<File>

    interface File {
        val hash: Hash?

        val path: String

        @Deprecated("make sequential")
        fun readBytes(): ByteArray
        fun fileSize(): Long
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

        @Deprecated("make sequential")
        override fun readBytes(): ByteArray = file.readBytes()
        override fun fileSize(): Long = file.toPath().fileSize()

        override fun hashCode(): Int = file.hashCode()
        override fun equals(other: Any?): Boolean = other is DeviceFile && file == other.file
    }


    override fun resolve(path: String): FileSystem.File {
        check(!File(path).isAbsolute) { "Path $path is not absolute" }
        return DeviceFile(root.resolve(path))
    }

    override fun walk(): Sequence<FileSystem.File> = root.walk().filter { it.isFile }.map { resolve(it.relativeTo(root).path) }
}