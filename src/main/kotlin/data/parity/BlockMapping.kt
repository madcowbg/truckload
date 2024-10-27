package data.parity

import data.repo.Repo
import data.storage.FileReference
import data.storage.LiveBlock
import java.io.File
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

@ExperimentalEncodingApi
class BlockMapping(val fileBlocks: List<LiveBlock>) {
    val blocksForFile: MutableMap<File, ArrayList<String>> = mutableMapOf()

    init {
        for (block in fileBlocks) {
            for (file in  block.files) {
                blocksForFile.computeIfAbsent(file.file) {ArrayList()}.add(Base64.encode(block.hash))
            }
        }
    }
}

@OptIn(ExperimentalEncodingApi::class)
fun naiveBlockMapping(repo: Repo, blockSize: Int = 1 shl 12 /* 4KB */): BlockMapping {
    val fileBlocks: MutableList<LiveBlock> = mutableListOf()
    for (storedFile in repo.storage) {
        val splitCnt = if (storedFile.size % blockSize == 0) {
            storedFile.size / blockSize
        } else {
            (storedFile.size / blockSize) + 1
        }

        for (idx in (0 until splitCnt)) {
            val ref = FileReference(
                storedFile.fileObject,
                storedFile.hash,
                idx * blockSize,
                ((1 + idx) * blockSize).coerceAtMost(storedFile.size)
            )
            val block = LiveBlock(blockSize, listOf(ref))

            fileBlocks.add(block)
        }
    }
    return BlockMapping(fileBlocks)
}