package data

import data.storage.FileSystem
import data.storage.Hash
import java.io.IOException
import kotlin.random.Random

class DummyFileSystem(
    nFiles: Int = 1000,
    meanSize: Int = 4000,
    stdSize: Int = 6000,
    filenameLength: Int = 100,
    seed: Long = 0
) : FileSystem {
    class DummyFile(override val path: String, override val fileSize: Long) : FileSystem.File {
        override val hash: Hash by lazy { Hash(dataInRange(0, fileSize)) }

        private val data by lazy {
            val generator = Random(path.hashCode())
            val data = ByteArray(fileSize.toInt())
            generator.nextBytes(data)
            return@lazy data
        }

        override fun dataInRange(from: Long, to: Long): ByteArray = data.sliceArray(from.toInt() until to.toInt())
    }

    private val files = mutableMapOf<String, DummyFile>()

    init {
        val generator = Random(seed)
        0.rangeUntil(nFiles).forEach {
            val path = String(CharArray(filenameLength) { ('A'..'z').random(generator) })
            val fileSize = (meanSize + (1 - 2 * generator.nextFloat()) * stdSize).toLong().coerceAtLeast(0)
            files[path] = DummyFile(path, fileSize)
        }
    }

    override fun resolve(path: String): FileSystem.File = files[path] ?: throw IOException("File not found: $path")
    override fun walk(): Sequence<FileSystem.File> = files.values.sortedBy { it.path }.asSequence()
}
