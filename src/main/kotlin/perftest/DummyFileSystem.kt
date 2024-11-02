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
    inner class DummyFile(override val path: String, override var fileSize: Long) : ReadonlyFileSystem.File {
        override val location: ReadonlyFileSystem = this@DummyFileSystem

        internal var data: ByteArray = ByteArray(fileSize.toInt()).also {
            val generator = Random(path.hashCode())
            generator.nextBytes(it)
        }
            set(value) {
                field = value
                fileSize = field.size.toLong()
                hash = Hash.digest(field)
            }

        override var hash: Hash = Hash.digest(dataInRange(0, fileSize))
        override fun dataInRange(from: Long, to: Long): ByteArray = data.sliceArray(from.toInt() until to.toInt())

        override fun toString(): String = "DummyFile[$fileSize, $path]"
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

    override fun resolve(path: String): ReadonlyFileSystem.File =
        files[path] ?: throw IOException("File not found: $path")

    override fun walk(): Sequence<ReadonlyFileSystem.File> = files.values.sortedBy { it.path }.asSequence()
    override fun digest(path: String): Hash? = files[path]?.hash
    override fun exists(path: String): Boolean = path in files

    fun removeRandomFile() {
        files.keys.remove(files.keys.first())
    }

    fun changeRandomFile() {
        val randomFile = files.values.first()
        randomFile.data = randomFile.data.sliceArray(0..1000)
    }
}
