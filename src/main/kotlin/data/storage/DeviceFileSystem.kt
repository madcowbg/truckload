package data.storage

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import kotlin.io.path.fileSize

class DeviceFileSystem(rootFolder: String) : ReadonlyFileSystem {
    internal val root: File = File(rootFolder)
    private val allFiles = mutableMapOf<String, DeviceFile>()

    inner class DeviceFile(internal val file: File, override val hash: Hash) : ReadonlyFileSystem.File {

        init {
            check(file.isAbsolute) { "File $file is not absolute!" }
        }

        override val location: ReadonlyFileSystem = this@DeviceFileSystem

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

        override fun toString(): String = file.toString()
    }

    override fun resolve(path: String): DeviceFile {
        return allFiles.computeIfAbsent(path) { root.resolve(File(path)).let { DeviceFile(it, it.digest()!!) } }
    }

    override fun walk(): Sequence<DeviceFile> =
        root.walk().filter { it.isFile }.mapNotNull { file ->
            val hash = file.digest() ?: return@mapNotNull null
            allFiles.computeIfAbsent(file.path) { DeviceFile(file, hash) }
        }
}

class WritableDeviceFileSystem(rootFolder: String): WritableFileSystem {
    val root: File = File(rootFolder)

    override fun copy(file: ReadonlyFileSystem.File, toPath: String) {
        val inputFile =
            file as? DeviceFileSystem.DeviceFile ?: throw IllegalArgumentException("cannot copy $file to $toPath from non-device file!")

        inputFile.file.copyTo(root.resolve(toPath), overwrite = true)
    }
}

private fun File.digest(): Hash? = try {
    Hash.digest(this.readBytes())
} catch (e: IOException) {
    null
}

