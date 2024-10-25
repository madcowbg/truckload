package data.parity

import data.repo.Repo
import data.repo.readFolder
import data.storage.FileReference
import data.storage.LiveBlock
import org.junit.jupiter.api.Test
import printable
import java.io.File

class BlockMapping(val fileBlocks: Map<ByteArray, LiveBlock>) {


}

class ParityTest {
    val BLOCK_SIZE = 1 shl 12 // 4KB

    @Test
    fun `load filesystem`() {
        val repo: Repo = readFolder(File("./.experiments/data"))

        val fileBlocks: MutableMap<ByteArray, LiveBlock> = mutableMapOf()
        for (storedFile in repo.storage) {
            val splitCnt = if (storedFile.size % BLOCK_SIZE == 0) {
                storedFile.size / BLOCK_SIZE
            } else {
                storedFile.size / BLOCK_SIZE + 1
            }

            for (idx in (0 until splitCnt)) {
                val ref = FileReference(
                    storedFile.fileObject,
                    idx * BLOCK_SIZE,
                    ((1 + idx) * BLOCK_SIZE).coerceAtMost(storedFile.size)
                )
                val block = LiveBlock(BLOCK_SIZE, listOf(ref))

                fileBlocks[block.hash] = block
            }
        }

        println("Loaded ${fileBlocks.size} blocks on filesystem")
        for (fileBlock in fileBlocks.values) {
            println("${fileBlock.hash.printable} ${fileBlock.files}")
        }
    }
}