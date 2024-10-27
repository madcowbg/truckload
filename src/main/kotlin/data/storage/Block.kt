package data.storage

import java.io.File

interface Block {
    val size: Int
    val hash: Hash
    val data: ByteArray
}

class FileReference(val file: File, val fileHash: Hash, val from: Int, val to: Int) {
    val size: Int = to - from
    val data: ByteArray by lazy {
        file.readBytes().copyOfRange(from, to) // fixme this can only read 2GB files
    }

    override fun toString(): String = "{$file from $from to $to}"
}


class LiveBlock(override val size: Int, val files: List<FileReference>) : Block {
    override val data: ByteArray
        get() = files.map { it.data }.reduce { a, b -> a.plus(b) } + ByteArray(size - files.sumOf { it.size })

    override val hash: Hash by lazy { Hash.digest(data) }
}

open class MemoryBlock(final override val data: ByteArray) : Block {
    override val hash: Hash by lazy { Hash.digest(data) }
    override val size: Int = data.size
}

class ParityBlock(data: ByteArray): MemoryBlock(data)