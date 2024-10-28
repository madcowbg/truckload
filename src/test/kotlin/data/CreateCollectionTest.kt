package data

import data.parity.BlockMapping
import data.parity.naiveBlockMapping
import data.parity.naiveParitySets
import data.repo.Repo
import data.repo.readFolder
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
        val repo: Repo = readFolder(currentCollectionFilesLocation)
        println("Mapping to blocks ...")
        val blockMapping: BlockMapping = naiveBlockMapping(repo)

        println("insert files in catalogue...")
        insertFilesInCatalogue(collectionRepo, repo)

        println("insert live block mapping...")
        insertLiveBlockMapping(collectionRepo, blockMapping)

        println("Calculating parity sets ...")
        val paritySets = naiveParitySets(blockMapping)

        println("insert computed parity sets...")
        insertComputedParitySets(collectionRepo, paritySets)

        println("done init!")
    }
}