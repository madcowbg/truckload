package data.parity

import data.storage.*

fun naiveBlockMapping(storage: Iterator<ReadonlyFileSystem.File>, blockSize: Int = 1 shl 12 /* 4KB */): List<LiveBlock> {
    val fileBlocks: MutableList<LiveBlock> = mutableListOf()
    for (storedFile in storage) {
        val splitCnt = if (storedFile.fileSize % blockSize == 0L) {
            storedFile.fileSize / blockSize
        } else {
            (storedFile.fileSize / blockSize) + 1
        }

        for (idx in (0 until splitCnt)) {
            val ref = FileReference(
                storedFile,
                storedFile.hash,
                idx * blockSize,
                ((1 + idx) * blockSize).coerceAtMost(storedFile.fileSize)
            )
            val block = LiveBlock(blockSize, listOf(ref))

            fileBlocks.add(block)
        }
    }
    return fileBlocks
}