package data.storage

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import kotlin.io.path.fileSize

class DeviceFileSystem(rootFolder: String) : FileSystem {
    private val root: File = File(rootFolder)
    private val allFiles = mutableMapOf<String, DeviceFile>()

    inner class DeviceFile(private val file: File, override val hash: Hash) : FileSystem.File {

        init {
            check(file.isAbsolute) { "File $file is not absolute!" }
        }

        override val path: String = file.relativeTo(root).path

        override val fileSize: Long = file.toPath().fileSize()

        override fun dataInRange(from: Long, to: Long): ByteArray {
            check(from in 0..to) { "invalid range to read data from $file -> $from..$to" }
            val size: Long = to - from
            check(size <= Int.MAX_VALUE) { "can't read $size bytes, max is ${Int.MAX_VALUE}" }
            val data = ByteArray(size.toInt())
            val openedFile = RandomAccessFile(file, "r")
            openedFile.seek(from)
            openedFile.readFully(data)
            return data
        }

        override fun hashCode(): Int = file.hashCode()
        override fun equals(other: Any?): Boolean = other is DeviceFile && file == other.file
    }


    override fun resolve(path: String): FileSystem.File {
        return allFiles.computeIfAbsent(path) { root.resolve(File(path)).let { DeviceFile(it, it.digest()!!) } }
    }

    override fun walk(): Sequence<FileSystem.File> =
        root.walk().filter { it.isFile }.mapNotNull { file ->
            val hash = file.digest() ?: return@mapNotNull null
            allFiles.computeIfAbsent(file.path) { DeviceFile(file, hash) }
        }
}

private fun File.digest(): Hash? = try {
    Hash.digest(this.readBytes())
} catch (e: IOException) {
    null
}
