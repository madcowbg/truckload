package data.parity

import data.storage.LiveBlock
import data.storage.ParityBlock
import kotlin.experimental.xor

@Deprecated("remove and rename file")
class ParitySet(val liveBlocks: List<LiveBlock>) {
    val parityBlock: ParityBlock = ParityBlock(xorAll(liveBlocks.map { it.data }))
}

fun xorAll(blocks: List<ByteArray>): ByteArray {
    val blockSize = blocks.map { it.size }.distinct().single() // ensures block size is the same
    val data = ByteArray(blockSize)
    blocks.forEach { block -> data.applyXor(block) }
    return data
}

private fun ByteArray.applyXor(other: ByteArray) {
    for (i in indices) {
        this[i] = this[i] xor other[i]
    }
}

fun restoreBlock(partialSet: List<ByteArray>): ByteArray =
    xorAll(partialSet)