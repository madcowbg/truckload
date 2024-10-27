package data

import data.repo.sql.CatalogueFile
import data.repo.sql.StoredRepo
import data.repo.sql.listOfIssues
import data.repo.sql.naiveInitializeRepo
import data.storage.DeviceFileSystem
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.test.Test


class BuildRepoTest {

    @Test
    fun `read folder on hd and create repo`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_build/.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val storedRepo: StoredRepo = StoredRepo.connect(repoPath)

        val location = DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        storedRepo.naiveInitializeRepo(location)

        storedRepo.listOfIssues().forEach { println(it.message) }
        transaction(storedRepo.db) {
            CatalogueFile.selectAll().forEach { println(it[CatalogueFile.path]) }
        }
    }
}