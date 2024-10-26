package data.parity

import data.repo.Repo
import data.repo.readFolder
import data.storage.Block
import data.storage.MemoryBlock
import org.junit.jupiter.api.Test
import printable
import java.io.File
import kotlin.test.assertContentEquals


class ParityTest {
    private val repo: Repo = readFolder(File("./.experiments/data"))
    private val blockMapping: BlockMapping = naiveBlockMapping(repo)
    private val paritySets = naiveParitySets(blockMapping)

    @Test
    fun `load from filesystem and dump them`() {
        println("Loaded ${blockMapping.fileBlocks.size} blocks on filesystem")
        println("file to blocks:")
        for ((file, blocks) in blockMapping.blocksForFile) {
            println("$file -> ${blocks.size}: $blocks")
        }
        println("block to file:")
        for (fileBlock in blockMapping.fileBlocks.values) {
            println("${fileBlock.hash.printable} ${fileBlock.files}")
        }
    }

    @Test
    fun `calculate add parity blocks and dump them`() {
        println("Parity sets:")
        for (paritySet in paritySets) {
            println("${paritySet.parityBlock.hash.printable}: ${paritySet.parityBlock.data.printable}")
            println(" protecting ${paritySet.liveBlocks.map { it.hash.printable }}")
        }
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    @Test
    fun `can restore from parity blocks`() {
        for (paritySet in paritySets) {
            for (i in paritySet.liveBlocks.indices) {
                println("restoring ${paritySet.parityBlock.hash.printable} for idx $i")
                val partialSet =
                    paritySet.liveBlocks.subList(0, i) + paritySet.liveBlocks.subList(i + 1, paritySet.liveBlocks.size)
                val restoredBlock = restoreBlock(partialSet + listOf(paritySet.parityBlock))

                assertContentEquals(
                    paritySet.liveBlocks[i].data.toUByteArray(),
                    restoredBlock.data.toUByteArray(),
                    "restore unsuccessful at idx $i"
                )
            }
            assertContentEquals(
                paritySet.parityBlock.data.toUByteArray(),
                restoreBlock(paritySet.liveBlocks).data.toUByteArray(),
                "restore unsuccessful for parity block"
            )
        }
    }

}