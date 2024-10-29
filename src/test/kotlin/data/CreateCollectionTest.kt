package data

import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.FileVersions
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.listOfIssues
import data.repo.sql.parity.ParityDataBlockMappings
import data.repo.sql.storagemedia.FileLocations
import data.repo.sql.storagemedia.ParityLocations
import data.repo.sql.storagemedia.StorageMedias
import data.storage.DeviceFileSystem
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
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

        // do full restore
        val newFromFullRestoreLocation = WritableDeviceFileSystem("$testPath/restore_from_full/")
        val restoreDevices = (0 until numPartitions)
            .associateWith { DeviceFileSystem("$testPath/Device ${it}/") }
        restoreCollection(collectionRepo, restoreDevices, newFromFullRestoreLocation)

        assertEqualsFileSystem(newFromFullRestoreLocation, currentCollectionFilesLocation)

        // do partial restore
        val newFromPartialRestoreLocation = WritableDeviceFileSystem("$testPath/restore_from_partial/")
        val restorePartialDevices = (1 until numPartitions)
            .associateWith { DeviceFileSystem("$testPath/Device ${it}/") }
        restoreCollection(collectionRepo, restorePartialDevices, newFromPartialRestoreLocation)

        assertEqualsFileSystem(newFromPartialRestoreLocation, currentCollectionFilesLocation)
    }
}

fun assertEqualsFileSystem(actual: ReadonlyFileSystem, expected: ReadonlyFileSystem) {
    var comparisons = 0
    expected.walk().forEach { file ->
        comparisons++
        assertTrue(actual.digest(file.path) != null, "After $comparisons comparisons, file $file missing in actual!")
        assertTrue(
            actual.existsWithHash(file.path, file.hash.storeable),
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

fun <StorageMedia> restoreCollection(
    collectionRepo: StoredRepo,
    storageDevices: Map<StorageMedia, ReadonlyFileSystem>,
    newLocation: WritableDeviceFileSystem
) {
    transaction(collectionRepo.db) {
        // fixme weird logic...
        val storageDeviceGUIDs =
            StorageMedias.selectAll()
                .associate { (it[StorageMedias.label].toInt() as StorageMedia) to it[StorageMedias.guid] }
        val storageDeviceByGuid = storageDevices.mapKeys { storageDeviceGUIDs[it.key] }

        FileVersions.selectAll().forEach { fileVersion ->
            val restorePath = fileVersion[FileVersions.path]
            // todo consider VersionState

            val fileHash = fileVersion[FileVersions.hash]

            val digest = newLocation.digest(restorePath)
            if (digest != null) {
                println("File exists: $restorePath, checking digest...")
            }

            if (digest?.storeable == fileHash) {
                println("File contents are the same!")
                return@forEach
            }

            val storedPossibleLocations = FileLocations.selectAll()
                .where { FileLocations.hash eq fileHash }
                .associate { it[FileLocations.storageMedia] to it[FileLocations.path] }
            println("Found ${storedPossibleLocations.size} possible stored locations!")

            val usableLocations = storedPossibleLocations.filter { (storageDeviceGUID, storageDevicePath) ->
                val device = storageDeviceByGuid[storageDeviceGUID]
                if (device == null) {
                    println("Device with guid $storageDeviceGUID missing! Can't use for restore.")
                    false
                } else {
                    device.existsWithHash(storageDevicePath, fileHash)
                }
            }

            if (usableLocations.isNotEmpty()) {
                for ((storageDeviceGUID, storageDevicePath) in usableLocations) {
                    val device = checkNotNull(storageDeviceByGuid[storageDeviceGUID])
                    newLocation.copy(device.resolve(storageDevicePath), restorePath)
                    return@forEach
                }
            }

            println("Backup file $restorePath is not available, restoring via parity checks.")

            val necessaryDataBlocks = FileDataBlockMappings.select(FileDataBlockMappings.dataBlockHash)
                .where { FileDataBlockMappings.fileHash eq fileHash }
                .map { it[FileDataBlockMappings.dataBlockHash] }

            println("Will restore file with ${necessaryDataBlocks.size} data blocks.")

            // fetch parity sets
            // fetch necessary live blocks (in memory blocks, for now)
            // do parity restore to recreate blocks
            // write from data from blocks

            throw NotImplementedError("Implement restore when data is unavailable")
        }
    }
}


private fun ReadonlyFileSystem.existsWithHash(
    path: String,
    fileHash: String
): Boolean {
    val existingDigest = this.digest(path)
    return if (existingDigest == null) {
        false
    } else {
        println("Found file to restore from... checking hash")
        if (existingDigest.storeable != fileHash) {
            println("BAD HASH!")
            false
        } else {
            println("Hash is correct! Using...")
            true
        }
    }
}
