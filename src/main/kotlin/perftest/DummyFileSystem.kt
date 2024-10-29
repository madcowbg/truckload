package perftest

import data.storage.ReadonlyFileSystem
import data.storage.Hash
import java.io.IOException
import kotlin.random.Random

class DummyFileSystem(
    nFiles: Int = 1000,
    meanSize: Int = 4000,
    stdSize: Int = 6000,
    filenameLength: Int = 100,
    seed: Long = 0
) : ReadonlyFileSystem {
    inner class DummyFile(override val path: String, override val fileSize: Long) : ReadonlyFileSystem.File {
        override val location: ReadonlyFileSystem = this@DummyFileSystem
        override val hash: Hash by lazy { Hash(dataInRange(0, fileSize)) }

//        private val data: ByteArray
//            get() {
//                val generator = Random(path.hashCode())
//                val data = ByteArray(fileSize.toInt())
//                generator.nextBytes(data)
//                return data
//            }

        private val data: ByteArray by lazy {
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
            val path = String(CharArray(filenameLength) { (('A'..'Z') + ('a'..'z') + ('/')).random(generator) })
            val fileSize = (meanSize + (1 - 2 * generator.nextFloat()) * stdSize).toLong().coerceAtLeast(0)
            files[path] = DummyFile(path, fileSize)
        }
    }

    override fun resolve(path: String): ReadonlyFileSystem.File = files[path] ?: throw IOException("File not found: $path")
    override fun walk(): Sequence<ReadonlyFileSystem.File> = files.values.sortedBy { it.path }.asSequence()
}