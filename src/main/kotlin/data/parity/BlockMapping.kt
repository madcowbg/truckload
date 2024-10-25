package data.parity

import data.repo.Repo
import data.storage.FileReference
import data.storage.LiveBlock
import java.io.File

class BlockMapping(val fileBlocks: Map<ByteArray, LiveBlock>) {
    val blocksForFile: MutableMap<File, ArrayList<ByteArray>> = mutableMapOf()

    init {
        for ((blockHash, block) in fileBlocks) {
            for (file in  block.files) {
                blocksForFile.computeIfAbsent(file.file) {ArrayList()}.add(blockHash)
            }
        }
    }
}

fun naiveBlockMapping(repo: Repo, blockSize: Int = 1 shl 12 /* 4KB */): BlockMapping {
    val fileBlocks: MutableMap<ByteArray, LiveBlock> = mutableMapOf()
    for (storedFile in repo.storage) {
        val splitCnt = if (storedFile.size % blockSize == 0) {
            storedFile.size / blockSize
        } else {
            storedFile.size / blockSize + 1
        }

        for (idx in (0 until splitCnt)) {
            val ref = FileReference(
                storedFile.fileObject,
                idx * blockSize,
                ((1 + idx) * blockSize).coerceAtMost(storedFile.size)
            )
            val block = LiveBlock(blockSize, listOf(ref))

            fileBlocks[block.hash] = block
        }
    }
    return BlockMapping(fileBlocks)
}