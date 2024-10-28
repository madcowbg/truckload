package data

import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.FileVersions
import data.repo.sql.listOfIssues
import data.repo.sql.naiveInitializeRepo
import data.storage.DeviceFileSystem
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import perftest.DummyFileSystem
import perftest.TestDataSettings
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.test.Test


class BuildRepoTest {

    @Test
    fun `read folder on hd and create repo`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_build/.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val storedRepo: StoredRepo = StoredRepo.connect(repoPath)

        val location = DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        storedRepo.naiveInitializeRepo(location) { msg -> Logger.getGlobal().log(Level.INFO, msg) }

        storedRepo.listOfIssues().forEach { println(it.message) }
        transaction(storedRepo.db) {
            FileVersions.selectAll().forEach { println(it[FileVersions.path]) }
        }
    }

    @Test
    fun `build in-mem repo`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_build_inmem/.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val storedRepo: StoredRepo = StoredRepo.connect(repoPath)

        val location = DummyFileSystem(nFiles = 1000, meanSize = 400, stdSize = 500, filenameLength = 100)

        storedRepo.naiveInitializeRepo(location) { msg -> Logger.getGlobal().log(Level.INFO, msg) }
        storedRepo.listOfIssues().forEach { println(it.message) }
    }
}