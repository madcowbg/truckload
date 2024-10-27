package data.parity

import data.repo.Repo
import data.repo.readFolder
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.test.assertContentEquals


@OptIn(ExperimentalEncodingApi::class)
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
        for (fileBlock in blockMapping.fileBlocks) {
            println("${fileBlock.hash} ${fileBlock.files}")
        }
    }

    @Test
    fun `calculate add parity blocks and dump them`() {
        println("Parity sets:")
        for (paritySet in paritySets) {
            println("${paritySet.parityBlock.hash}: ${paritySet.parityBlock.data}")
            println(" protecting ${paritySet.liveBlocks.map { it.hash }}")
        }
    }

    @Test
    fun `can restore from parity blocks`() {
        for (paritySet in paritySets) {
            for (i in paritySet.liveBlocks.indices) {
                println("restoring ${paritySet.parityBlock.hash} for idx $i")
                val partialSet =
                    paritySet.liveBlocks.subList(0, i) + paritySet.liveBlocks.subList(i + 1, paritySet.liveBlocks.size)
                val restoredBlock = restoreBlock(partialSet + listOf(paritySet.parityBlock))

                assertContentEquals(
                    paritySet.liveBlocks[i].data,
                    restoredBlock.data,
                    "restore unsuccessful at idx $i"
                )
            }
            assertContentEquals(
                paritySet.parityBlock.data,
                restoreBlock(paritySet.liveBlocks).data,
                "restore unsuccessful for parity block"
            )
        }
    }

}