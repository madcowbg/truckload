package data

import data.repo.sql.StoredRepo
import data.repo.sql.listOfIssues
import data.storage.DeviceFileSystem
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Test
import perftest.TestDataSettings
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CreateCollectionTest {

    @Test
    fun `create repo and split into storage media`() {
        val testPath = "${TestDataSettings.test_path}/.experiments/test_collection"
        File(testPath).let { if (it.exists()) it.deleteRecursively() }

        val repoPath = "$testPath/.repo"
        StoredRepo.delete(repoPath)

        val collectionRepo: StoredRepo = StoredRepo.init(repoPath)

        val currentCollectionFilesLocation: ReadonlyFileSystem =
            DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        val numPartitions = 6
        val storageDevices = (0 until numPartitions)
            .associateWith { WritableDeviceFileSystem("$testPath/Device ${it}/") }

        storeCollectionBackup(collectionRepo, currentCollectionFilesLocation, storageDevices)

        assertEquals(0, collectionRepo.listOfIssues().count())
        for (issue in collectionRepo.listOfIssues()) {
            println(issue)
        }
        val logger: KLogger = KotlinLogging.logger {}

        // do full restore
        val newFromFullRestoreLocation = WritableDeviceFileSystem("$testPath/restore_from_full/")
        val restoreDevices = (0 until numPartitions)
            .associateWith { DeviceFileSystem("$testPath/Device ${it}/") }
        restoreCollection(collectionRepo, restoreDevices, newFromFullRestoreLocation, logger)

        assertEqualsFileSystem(newFromFullRestoreLocation, currentCollectionFilesLocation, logger)

        // do partial restore
        val newFromPartialRestoreLocation = WritableDeviceFileSystem("$testPath/restore_from_partial/")
        val restorePartialDevices = (1 until numPartitions)
            .associateWith { DeviceFileSystem("$testPath/Device ${it}/") }
        restoreCollection(collectionRepo, restorePartialDevices, newFromPartialRestoreLocation, logger)

        assertEqualsFileSystem(newFromPartialRestoreLocation, currentCollectionFilesLocation, logger)
    }
}

fun assertEqualsFileSystem(actual: ReadonlyFileSystem, expected: ReadonlyFileSystem, logger: KLogger) {
    var comparisons = 0
    expected.walk().forEach { file ->
        comparisons++
        assertTrue(actual.digest(file.path) != null, "After $comparisons comparisons, file $file missing in actual!")
        assertTrue(
            actual.existsWithHash(file.path, file.hash.storeable, logger),
            "After $comparisons comparisons, file ${file.path} has different hash!"
        )
    }

    actual.walk().forEach { file ->
        comparisons++
        assertTrue(
            expected.digest(file.path) != null,
            "After $comparisons comparisons, file $file in actual should not exist!"
        )
    }
    println("Done $comparisons comparisons, all good!")
}


