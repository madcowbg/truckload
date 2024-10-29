package data

import data.repo.sql.StoredRepo
import data.storage.DeviceFileSystem
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.junit.jupiter.api.Test
import perftest.TestDataSettings
import java.io.File

class CreateCollectionTest {

    @Test
    fun `create repo and split into storage media`() {
        val testPath = "${TestDataSettings.test_path}/.experiments/test_collection"
        File(testPath).let {if (it.exists()) it.deleteRecursively()}

        val repoPath = "$testPath/.repo"
        StoredRepo.delete(repoPath)

        val collectionRepo: StoredRepo = StoredRepo.init(repoPath)

        val currentCollectionFilesLocation: ReadonlyFileSystem =
            DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        val numPartitions = 6
        val storageDevices = (0 until numPartitions)
            .associateWith { WritableDeviceFileSystem("$testPath/Device ${it}/") }

        storeCollectionBackup(collectionRepo, currentCollectionFilesLocation, storageDevices)
    }
}

