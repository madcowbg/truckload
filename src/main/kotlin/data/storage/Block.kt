package data.storage

import java.io.File
import java.security.MessageDigest

interface Block {
    val size: Int
    val hash: ByteArray
}

class BlankReference(val size: Int) {
    val data: ByteArray
        get() = ByteArray(size)
}

class FileReference(val file: File, val from: Int, val to: Int) {
    val size: Int = to - from
    val data: ByteArray by lazy {
        file.readBytes().copyOfRange(from, to) // fixme this can only read 2GB files
    }

    override fun toString(): String = "{$file from $from to $to}"
}

private val md5 = MessageDigest.getInstance("md5")

class LiveBlock(override val size: Int, val files: List<FileReference>) : Block {
    val data: ByteArray
        get() = files.map { it.data }.reduce { a, b -> a.plus(b) } + ByteArray(size - files.sumOf { it.size })

    override val hash: ByteArray by lazy { md5.digest(data) }
}

class ParityBlock(val data: ByteArray) : Block {
    override val hash: ByteArray by lazy { md5.digest(data) }
    override val size: Int = data.size
}