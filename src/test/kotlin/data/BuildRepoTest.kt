package data

import data.repo.sql.StoredRepo
import data.repo.sql.listOfIssues
import data.repo.sql.naiveInitializeRepo
import data.storage.DeviceFileSystem
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
    }

    @Test
    fun `build in-mem repo`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_build_inmem/.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val storedRepo: StoredRepo = StoredRepo.connect(repoPath)

        val location = DummyFileSystem(nFiles = 1000, meanSize = 100, stdSize = 500, filenameLength = 100)

        storedRepo.naiveInitializeRepo(location)
        storedRepo.listOfIssues().forEach { println(it.message) }
    }
}