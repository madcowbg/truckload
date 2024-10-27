package data.storage

interface Block {
    val size: Int
    val hash: Hash
    val data: ByteArray
}

class FileReference(val file: FileSystem.File, val fileHash: Hash, val from: Long, val to: Long) {
    val size: Long = to - from
    val data: ByteArray
        get() = file.dataInRange(from, to)

    override fun toString(): String = "{$file from $from to $to}"
}

class LiveBlock(override val size: Int, val files: List<FileReference>) : Block {
    override val data: ByteArray
        get() = files.map { it.data }.reduce { a, b -> a.plus(b) } + ByteArray((size - files.sumOf { it.size }).toInt())

    override val hash: Hash by lazy { Hash.digest(data) }
}

open class MemoryBlock(final override val data: ByteArray) : Block {
    override val hash: Hash by lazy { Hash.digest(data) }
    override val size: Int = data.size
}

class ParityBlock(data: ByteArray): MemoryBlock(data)