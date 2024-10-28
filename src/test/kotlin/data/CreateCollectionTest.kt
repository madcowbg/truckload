package data

import data.parity.naiveBlockMapping
import data.repo.sql.*
import data.storage.DeviceFileSystem
import org.junit.jupiter.api.Test
import perftest.TestDataSettings

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
        val blockMappings = partitionedFiles.mapValues { naiveBlockMapping(it.value.iterator()) }

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

