package data.parity

import data.storage.Block
import data.storage.LiveBlock
import data.storage.MemoryBlock
import data.storage.ParityBlock
import printable
import kotlin.experimental.xor


class ParitySet(val liveBlocks: List<LiveBlock>) {
    val parityBlock: ParityBlock = ParityBlock(xorAll(liveBlocks))
}

fun xorAll(blocks: List<Block>): ByteArray {
    val blockSize = blocks.map { it.size }.distinct().single() // ensures block size is the same
    val data = ByteArray(blockSize)
    blocks.forEach { block -> data.applyXor(block.data) }
    return data
}

private fun ByteArray.applyXor(other: ByteArray) {
    for (i in indices) {
        this[i] = this[i] xor other[i]
    }
}

fun naiveParitySets(blockMapping: BlockMapping): List<ParitySet> {
    val paritySetSize = 4
    val liveBlocks = blockMapping.fileBlocks.values.sortedBy { it.hash.printable } // fixme stupid way to sort...
    val setsCnt = (liveBlocks.size - 1) / paritySetSize + 1
    val paritySets = (0 until setsCnt).map {
        ParitySet(
            liveBlocks = liveBlocks.subList(
                it * paritySetSize,
                ((1 + it) * paritySetSize).coerceAtMost(liveBlocks.size)
            )
        )
    }
    return paritySets
}

fun restoreBlock(partialSet: List<Block>): MemoryBlock =
    MemoryBlock(xorAll(partialSet))