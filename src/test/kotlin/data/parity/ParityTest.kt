package data.parity

import data.repo.Repo
import data.repo.readFolder
import org.junit.jupiter.api.Test
import printable
import java.io.File



class ParityTest {
    private val repo: Repo = readFolder(File("./.experiments/data"))
    private val blockMapping: BlockMapping = naiveBlockMapping(repo)

    @Test
    fun `load filesystem`() {
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
    fun `add parity blocks`() {
        val paritySets = naiveParitySets(blockMapping)

        println("Parity sets:")
        for (paritySet in paritySets) {
            println("${paritySet.parityBlock.hash.printable}: ${paritySet.parityBlock.data.printable}")
            println(" protecting ${paritySet.liveBlocks.map { it.hash.printable }}")
        }
    }
}