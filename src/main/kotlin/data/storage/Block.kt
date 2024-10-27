package data.storage

interface Block {
    val size: Long
    val hash: Hash
    val data: ByteArray
}

class FileReference(val file: FileSystem.File, val fileHash: Hash, val from: Long, val to: Long) {
    val size: Long = to - from
    val data: ByteArray by lazy {
        file.readBytes().copyOfRange(from.toInt(), to.toInt()) // fixme this can only read 2GB files
    }

    override fun toString(): String = "{$file from $from to $to}"
}


class LiveBlock(override val size: Long, val files: List<FileReference>) : Block {
    override val data: ByteArray
        get() = files.map { it.data }.reduce { a, b -> a.plus(b) } + ByteArray((size - files.sumOf { it.size }).toInt())

    override val hash: Hash by lazy { Hash.digest(data) }
}

open class MemoryBlock(final override val data: ByteArray) : Block {
    override val hash: Hash by lazy { Hash.digest(data) }
    override val size: Long = data.size.toLong()  // FIXME make long to support files >2GB
}

class ParityBlock(data: ByteArray): MemoryBlock(data)