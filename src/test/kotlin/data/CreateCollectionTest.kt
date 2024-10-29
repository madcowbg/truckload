package data

import data.parity.restoreBlock
import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.FileVersions
import data.repo.sql.datablocks.DataBlocks
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.listOfIssues
import data.repo.sql.parity.ParityBlocks
import data.repo.sql.parity.ParityDataBlockMappings
import data.repo.sql.parity.ParitySets
import data.repo.sql.storagemedia.FileLocations
import data.repo.sql.storagemedia.ParityLocations
import data.repo.sql.storagemedia.StorageMedias
import data.storage.DeviceFileSystem
import data.storage.Hash
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.jetbrains.exposed.sql.JoinType
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
//            println("Found ${storedPossibleLocations.size} possible stored locations!")

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

            data class NecessaryDataBlockMappings(
                val fileOffset: Long,
                val chunkSize: Int,
                val dataBlockHash: String,
                val blockOffset: Int,
            )

            val necessaryDataBlocks: List<NecessaryDataBlockMappings> =
                FileDataBlockMappings.selectAll()
                    .where { FileDataBlockMappings.fileHash eq fileHash }
                    .map {
                        NecessaryDataBlockMappings(
                            it[FileDataBlockMappings.fileOffset],
                            it[FileDataBlockMappings.chunkSize],
                            it[FileDataBlockMappings.dataBlockHash],
                            it[FileDataBlockMappings.blockOffset],
                        )
                    }

            println("Will restore file $fileHash with ${necessaryDataBlocks.size} data blocks.")

            data class ParitySetToRestore(val paritySetId: String)

            // parity blocks
            val paritySetIds: List<ParitySetToRestore> =
                (FileDataBlockMappings innerJoin DataBlocks innerJoin ParityDataBlockMappings innerJoin ParitySets)
                    .selectAll() // fixme optimise
                    .where { FileDataBlockMappings.fileHash eq fileHash }
                    .map { ParitySetToRestore(it[ParitySets.hash]) }

            data class ParityBlockToRestore(
                val paritySetId: String,
                val parityPHash: String,
                val parityFile: ReadonlyFileSystem.File?
            )

            val parityRestoreBlocks: List<ParityBlockToRestore> =
                (ParitySets.join(
                    ParityBlocks,
                    JoinType.INNER,
                    onColumn = ParityBlocks.hash,
                    otherColumn = ParitySets.parityPHash
                ) innerJoin ParityLocations innerJoin StorageMedias)
                    .selectAll() // fixme optimise
                    .where { ParitySets.hash.inList(paritySetIds.map { it.paritySetId }) }
                    .map {
                        val storageDevice = storageDeviceByGuid[it[StorageMedias.guid]]
                        val parityLocationFilePath =
                            storageDevice?.resolve(".repo/parity_blocks/${it[ParityLocations.hash]}.parity")
                        ParityBlockToRestore(
                            it[ParitySets.hash],
                            it[ParitySets.parityPHash],
                            parityLocationFilePath
                        )
                    }

            parityRestoreBlocks.forEach { println(it) }

            data class LiveDataToRestore(
                val paritySetId: String,
                val fileHash: String,
                val fileOffset: Long,
                val chunkSize: Int,
                val dataBlockHash: String,
                val dataBlockSize: Int,
                val blockOffset: Int,
            )

            val dataRestoreBlocks: Map<String, LiveDataToRestore> =
                (ParityDataBlockMappings innerJoin DataBlocks innerJoin FileDataBlockMappings innerJoin FileRefs)
                    .selectAll() // fixme optimise
                    .where { ParityDataBlockMappings.paritySetId.inList(paritySetIds.map { it.paritySetId }) }
                    .associate {
                        it[FileDataBlockMappings.fileHash] to LiveDataToRestore(
                            it[ParityDataBlockMappings.paritySetId],
                            it[FileDataBlockMappings.fileHash],
                            it[FileDataBlockMappings.fileOffset],
                            it[FileDataBlockMappings.chunkSize],
                            it[FileDataBlockMappings.dataBlockHash],
                            it[DataBlocks.size],
                            it[FileDataBlockMappings.blockOffset]
                        )
                    }
            dataRestoreBlocks.forEach { println(it) }

            data class RestoreLocation(
                val fileHash: String,
                val storageDeviceGUID: String,
                val fileObject: ReadonlyFileSystem.File
            )

            val restoreLocations = (FileLocations innerJoin StorageMedias).selectAll()
                .where { FileLocations.hash.inList(dataRestoreBlocks.keys) }
                .mapNotNull {
                    val storageDeviceGUID = it[StorageMedias.guid]
                    val storageDevice = storageDeviceByGuid[storageDeviceGUID] ?: return@mapNotNull null
                    val storageLocationFilePath = it[FileLocations.path]

                    // this can be used
                    RestoreLocation(it[FileLocations.hash], storageDeviceGUID, storageDevice.resolve(storageLocationFilePath))
                }.groupBy { it.fileHash }

            restoreLocations.forEach { println(it) }

            data class LiveToRestoreFile(val data: LiveDataToRestore, val file: ReadonlyFileSystem.File?)
            val dataRestoreFiles = dataRestoreBlocks.map { (key, data) ->
                val restoreFile = restoreLocations[key]?.firstOrNull()?.fileObject
                if (restoreFile == null) {
                    println("Cannot find a viable source for file $key to restore from.")
                }
                LiveToRestoreFile(data, restoreFile)
            }

            dataRestoreFiles.forEach { println(it) }

            val parityRestoreBlocksByParitySet = parityRestoreBlocks.groupBy { it.paritySetId }
            val dataRestoreBlocksByParitySet = dataRestoreFiles.groupBy { it.data.paritySetId }

            val restoredBlocks: Map<String, ByteArray> =
                dataRestoreBlocksByParitySet.mapNotNull { (paritySetId, liveBlocks) ->
                    if (!liveBlocks.any { liveBlock -> liveBlock.data.fileHash == fileHash }) {
                        println("Skipping setId=$paritySetId because we don't need any of the blocks.")
                        throw IllegalStateException("should never happen")
                    }

                    println("Restoring setId=" + paritySetId + " " + liveBlocks.size)

                    val parityBlocks = parityRestoreBlocksByParitySet[paritySetId]!! // todo what if null??
                    val parityBlocksData = parityBlocks
                        .mapNotNull { parityBlock -> parityBlock.parityFile?.let { it.dataInRange(0, it.fileSize) } }

                    val filesByBlock = liveBlocks.groupBy { it.data.dataBlockHash }
                    val filesBlockData = filesByBlock.mapValues { (_, filesForBlock) ->
                        val blockData = ByteArray(filesForBlock.first().data.dataBlockSize)
                        for (it in filesForBlock) {
                            if (it.data.fileHash == fileHash) {
                                println("Skipping ${it.data.fileHash} because it is in fact the block we want to restore") // ugly AF
                                continue
                            }
                            if (it.file == null) {
                                throw IllegalStateException("File missing: ${it.data.fileHash} but is required for restore!")
                            }

                            val fileData =
                                it.file.dataInRange(it.data.fileOffset, it.data.fileOffset + it.data.chunkSize)
                            fileData.copyInto(blockData, it.data.blockOffset)
                        }
                        return@mapValues blockData
                    }

                    val restoredBlock = restoreBlock(parityBlocksData + filesBlockData.values)
                    val restoredHash = Hash.digest(restoredBlock)
                    if (liveBlocks.map { it.data.dataBlockHash }.contains(restoredHash.storeable)) {
                        println("Successful restore of block $restoredHash!")
                    } else {
                        throw IllegalStateException("Failed restore of parityId $paritySetId!")
                    }
                    restoredHash.storeable to restoredBlock
                }.toMap()

            val unrecoverableBlocks = necessaryDataBlocks.filter { it.dataBlockHash !in restoredBlocks.keys }
            unrecoverableBlocks.forEach {
                println("Failed restoring block $it")
            }

            val necessaryDataBlockHashes = necessaryDataBlocks.map { it.dataBlockHash }
            restoredBlocks.keys.filter { it !in necessaryDataBlockHashes }.forEach {
                println("Restored unnecessary block $it, but it is fine.")
            }

            val restoredFileData =
                ByteArray(fileVersion[FileRefs.size].toInt()) // todo rewrite on-line to support >2GB files
            necessaryDataBlocks.forEach { blockDescriptor ->
                val restoredBlockData = checkNotNull(restoredBlocks[blockDescriptor.dataBlockHash])

                restoredBlockData
                    .sliceArray(blockDescriptor.blockOffset until blockDescriptor.blockOffset + blockDescriptor.chunkSize)
                    .copyInto(restoredFileData, blockDescriptor.fileOffset.toInt())
            }

            val restoredFileDigest = Hash.digest(restoredFileData)
            if (restoredFileDigest.storeable != fileHash) {
                throw IllegalStateException("Restored file does not have expected hash!")
            }

            newLocation.write(fileVersion[FileVersions.path], restoredFileData)

//
//                .join(ParityDataBlockMappings, JoinType.INNER, onColumn = DataBlocks.hash, otherColumn = Parity)
//                .selectAll()

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
//        println("Found file to restore from... checking hash")
        if (existingDigest.storeable != fileHash) {
            println("BAD HASH for $path!")
            false
        } else {
//            println("Hash is correct! Using...")
            true
        }
    }
}
