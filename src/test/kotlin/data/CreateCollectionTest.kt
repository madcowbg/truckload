package data

import data.parity.BlockMapping
import data.parity.naiveParitySets
import data.repo.sql.*
import data.storage.DeviceFileSystem
import data.storage.FileReference
import data.storage.FileSystem
import data.storage.LiveBlock
import org.junit.jupiter.api.Test
import perftest.TestDataSettings

fun blockMapping(storage: List<FileSystem.File>, blockSize: Int = 1 shl 12 /* 4KB */): BlockMapping {
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
    return BlockMapping(fileBlocks)
}

class CreateCollectionTest {

    @Test
    fun `create repo and split into storage media`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_collection/.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val collectionRepo: StoredRepo = StoredRepo.connect(repoPath)

        val currentCollectionFilesLocation = DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        println("Reading folder ...")
        val storedFiles = currentCollectionFilesLocation.walk()

        val numPartitions = 6
        val partitionedFiles = storedFiles.groupBy {it.hash.hashCode() % numPartitions}

        println("Mapping to blocks ...")
        val blockMappings = partitionedFiles.mapValues {blockMapping(it.value)}

//        println("insert files in catalogue...")
//        insertFilesInCatalogue(collectionRepo, storedFiles )
//
//        println("insert live block mapping...")
//        insertLiveBlockMapping(collectionRepo, blockMapping)
//
//        println("Calculating parity sets ...")
//        val paritySets = naiveParitySets(blockMapping)
//
//        println("insert computed parity sets...")
//        insertComputedParitySets(collectionRepo, paritySets)

        println("done init!")
    }
}

