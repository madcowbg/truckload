package data

import data.parity.restoreBlock
import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.CatalogueFileVersions
import data.repo.sql.datablocks.FileDataBlocks
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.listOfIssues
import data.repo.sql.parity.ParityBlocks
import data.repo.sql.parity.ParitySetFileDataBlockMapping
import data.repo.sql.parity.ParitySets
import data.repo.sql.storagemedia.StorageFileLocations
import data.repo.sql.storagemedia.StorageParityLocations
import data.repo.sql.storagemedia.StorageMedias
import data.storage.DeviceFileSystem
import data.storage.Hash
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.jetbrains.exposed.sql.JoinType
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
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


data class FileVersion(val path: String, val hash: String, val size: Long)

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
        val storageDeviceByGuid = storageDevices.mapKeys { storageDeviceGUIDs[it.key]!! }

        (CatalogueFileVersions innerJoin FileRefs).selectAll().forEach { fileVersion ->
            restoreFile(
                storageDeviceByGuid,
                FileVersion(
                    fileVersion[CatalogueFileVersions.path],
                    fileVersion[CatalogueFileVersions.hash],
                    fileVersion[FileRefs.size]
                ),
                newLocation
            )
        }
    }
}

fun restoreFile(
    storageDeviceByGuid: Map<String, ReadonlyFileSystem>,
    fileVersion: FileVersion,
    newLocation: WritableDeviceFileSystem
) {
    val digest = newLocation.digest(fileVersion.path)
    if (digest != null) {
        println("File exists: ${fileVersion.path}, checking digest...")
    }

    if (digest?.storeable == fileVersion.hash) {
        println("File contents are the same!")
        return
    }

    val storedPossibleLocations = StorageFileLocations.selectAll()
        .where { StorageFileLocations.hash eq fileVersion.hash }
        .associate { it[StorageFileLocations.storageMedia] to it[StorageFileLocations.path] }
//    println("Found ${storedPossibleLocations.size} possible stored locations!")

    val usableLocations = storedPossibleLocations.mapNotNull { (storageDeviceGUID, storageDevicePath) ->
        val device = storageDeviceByGuid[storageDeviceGUID]
        if (device == null) {
            println("Device with guid $storageDeviceGUID missing! Can't use for restore.")
            return@mapNotNull null
        } else {
            if (device.existsWithHash(storageDevicePath, fileVersion.hash)) {
                return@mapNotNull device.resolve(storageDevicePath)
            }
        }
        return@mapNotNull null
    }

    if (usableLocations.isNotEmpty()) {
        for (file in usableLocations) {
            newLocation.copy(file, fileVersion.path) // todo add error handling
            return
        }
    }

    println("Backup file ${fileVersion.path} is not available, restoring via parity checks.")

    println("Finding out what data blocks we need to restore the file...")
    data class FileDataBlocksToRestore(
        val fileOffset: Long,
        val chunkSize: Int,
        val dataBlockHash: String,
        val blockOffset: Int,
    )

    val necessaryDataBlocks: List<FileDataBlocksToRestore> =
        FileDataBlockMappings.selectAll()
            .where { FileDataBlockMappings.fileHash eq fileVersion.hash }
            .map {
                FileDataBlocksToRestore(
                    it[FileDataBlockMappings.fileOffset],
                    it[FileDataBlockMappings.chunkSize],
                    it[FileDataBlockMappings.dataBlockHash],
                    it[FileDataBlockMappings.blockOffset],
                )
            }

    println("Will restore file ${fileVersion.hash} with ${necessaryDataBlocks.size} data blocks.")

    data class ParitySetDefinition(
        val paritySetId: String,
        val blockSize: Int,
        val parityPHash: String,
        val numDeviceBlocks: Int
    )

    println("Finding parity sets...")
    val paritySetsDefinitionForNecessaryDataBlocks: List<ParitySetDefinition> =
        (ParitySetFileDataBlockMapping innerJoin ParitySets).selectAll()
            .where(ParitySetFileDataBlockMapping.dataBlockHash.inList(necessaryDataBlocks.map { it.dataBlockHash }))
            .map {
                ParitySetDefinition(
                    it[ParitySets.hash],
                    it[ParitySets.blockSize],
                    it[ParitySets.parityPHash],
                    it[ParitySets.numDeviceBlocks]
                )
            }.distinct()

//    paritySetsDefinitionForNecessaryDataBlocks.forEach(::println)

    data class ParitySetConstituents(
        val paritySet: ParitySetDefinition,
        val dataBlockHash: String,
        val indexInSet: Int,
    )
    println("Loading all parity sets data...")
    val paritySetsConstituents = (ParitySetFileDataBlockMapping innerJoin ParitySets).selectAll()
        .where(ParitySetFileDataBlockMapping.paritySetId.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId }))
        .map {
            ParitySetConstituents(
                ParitySetDefinition(
                    it[ParitySets.hash],
                    it[ParitySets.blockSize],
                    it[ParitySets.parityPHash],
                    it[ParitySets.numDeviceBlocks]
                ),
                it[ParitySetFileDataBlockMapping.dataBlockHash],
                it[ParitySetFileDataBlockMapping.indexInSet],
            )
        }

//    paritySetsConstituents.forEach(::println)

    data class ParityBlocksForRestore(
        val paritySetId: String,
        val hash: String,
        val file: ReadonlyFileSystem.File?
    )
    println("Loading parity blocks information...")
    val parityBlocksForRestore = (ParitySets.join(
        ParityBlocks,
        JoinType.INNER,
        onColumn = ParitySets.parityPHash,
        otherColumn = ParityBlocks.hash
    ) innerJoin StorageParityLocations)
        .selectAll()
        .where { ParitySets.hash.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId }) }
        .map {
            val storageDeviceGUID = it[StorageParityLocations.storageMedia]
            val storageDevice = storageDeviceByGuid[storageDeviceGUID]

            ParityBlocksForRestore(
                it[ParitySets.hash],
                it[ParityBlocks.hash],
                storageDevice?.resolve(".repo/parity_blocks/${it[ParityBlocks.hash]}.parity")
            )
        }
//    parityBlocksForRestore.forEach(::println)

    data class FileDataBlockForRestore(
        val paritySetId: String,
        val fileHash: String,
        val fileOffset: Long,
        val chunkSize: Int,
        val dataBlockHash: String,
//                val dataBlockSize: Int,
        val blockOffset: Int,
        val storageDeviceGUID: String,
        val filePath: String,
        val fileObject: ReadonlyFileSystem.File?
    )
    println("Loading file blocks information necessary for restore")
    val fileDataBlocksForRestore =
        (ParitySetFileDataBlockMapping innerJoin FileDataBlocks innerJoin FileDataBlockMappings innerJoin FileRefs
                innerJoin StorageFileLocations)
            .selectAll()
            .where(ParitySetFileDataBlockMapping.paritySetId.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId }))
            .map {
                val storageDeviceGUID = it[StorageFileLocations.storageMedia]
                val storageDevice = storageDeviceByGuid[storageDeviceGUID]
                val storageLocationFilePath = it[StorageFileLocations.path]

                FileDataBlockForRestore(
                    it[ParitySetFileDataBlockMapping.paritySetId],
                    it[FileDataBlockMappings.fileHash],
                    it[FileDataBlockMappings.fileOffset],
                    it[FileDataBlockMappings.chunkSize],
                    it[FileDataBlockMappings.dataBlockHash],
//                            it[FileDataBlocks.size],
                    it[FileDataBlockMappings.blockOffset],
                    it[StorageFileLocations.storageMedia],
                    storageLocationFilePath,
                    storageDevice?.resolve(storageLocationFilePath)
                )
            }

//    fileDataBlocksForRestore.forEach(::println)

//            val parityRestoreBlocksByParitySet = parityRestoreBlocks.groupBy { it.paritySetId }
//            val dataRestoreBlocksByParitySet = dataRestoreFiles.groupBy { it.data.paritySetId }

    val paritySetsPerBlockHash = paritySetsConstituents
        .groupBy { it.dataBlockHash }
        .mapValues { (_, constituents) -> constituents.map { it.paritySet }.distinct() }

    val paritySetsConstituentsBySetId: Map<String, List<ParitySetConstituents>> =
        paritySetsConstituents.groupBy { it.paritySet.paritySetId }
//    val paritySetDescParSetId = paritySetsDefinitionForNecessaryDataBlocks.associateBy { it.paritySetId }
//    val paritySetsDataToRestorePerDataBlock = paritySetsConstituents.groupBy { it.dataBlockHash }
    val fileDataBlocksForRestorePerParitySetId = fileDataBlocksForRestore.groupBy { it.paritySetId }
    val parityBlocksForRestorePerParitySetId = parityBlocksForRestore.groupBy { it.paritySetId }

    val restoredBlocks: Map<String, ByteArray> =
        necessaryDataBlocks.map restoreLiveBlock@{ liveBlock ->
            val setsToUseForBlock: List<ParitySetDefinition> =
                paritySetsPerBlockHash[liveBlock.dataBlockHash]
                    ?: throw IllegalStateException("Cannot restore block ${liveBlock.dataBlockHash}, no parity set to use!")

            println("Trying restore with ${setsToUseForBlock.size} potential blocks...")
            setsToUseForBlock
                .forEach { paritySet ->
//                    val parityDescSet = paritySetsConstituentsBySetId[paritySet.paritySetId] ?: throw IllegalStateException("Missing set $paritySetId!")
                    println("Restoring by ${paritySet.paritySetId} with block size ${paritySet.blockSize}")

                    val fileDataBlocks =
                        fileDataBlocksForRestorePerParitySetId[paritySet.paritySetId] ?: return@forEach
                    println("Restoring by ${paritySet.paritySetId} with ${fileDataBlocks.size} potential file blocks...")

                    val filesBlockData: Map<String, ByteArray> = fileDataBlocks.groupBy { it.dataBlockHash }
                        .mapNotNull { (dataBlockHash, filesForBlockMultiDevices) ->
                            println("Restoring with $dataBlockHash with ${filesForBlockMultiDevices.size} potential blocks...")
//                            filesForBlockMultiDevices.forEach(::println)

                            filesForBlockMultiDevices.groupBy { it.storageDeviceGUID }
                                .mapNotNull restoringOnDevice@{ (storageDeviceGUID, filesForBlock) ->
                                    println("Using $storageDeviceGUID with ${filesForBlock.size} files to restore...")
//                                    filesForBlock.forEach(::println)

                                    val blockData = ByteArray(paritySet.blockSize)
                                    for (it in filesForBlock) {
                                        if (it.fileObject == null) {
                                            println("missing file ${it.filePath} on ${it.storageDeviceGUID}")
                                            return@restoringOnDevice null
                                        }

                                        val fileData =
                                            it.fileObject.dataInRange(it.fileOffset, it.fileOffset + it.chunkSize)
                                        fileData.copyInto(blockData, it.blockOffset)
                                    }
                                    val restoredBlockHash = Hash.digest(blockData).storeable
                                    check(restoredBlockHash == dataBlockHash) {
                                        "Restored digest $restoredBlockHash != $dataBlockHash!"
                                    }
                                    return@restoringOnDevice restoredBlockHash to blockData
                                }.firstOrNull()
                        }.toMap()

                    val availableRecoveredBlock = filesBlockData[liveBlock.dataBlockHash]
                    if (availableRecoveredBlock != null) {
                        println("File block is actually available! Returning...")
                        return@restoreLiveBlock liveBlock.dataBlockHash to availableRecoveredBlock
                    }

                    val parityBlocks = parityBlocksForRestorePerParitySetId[paritySet.paritySetId] ?: return@forEach
                    println("Restoring by ${paritySet.paritySetId} with ${fileDataBlocks.size} potential parity blocks...")

                    val parityBlocksData = parityBlocks
                        .mapNotNull { parityBlock ->
                            parityBlock.file?.let {
                                parityBlock.hash to it.dataInRange(
                                    0,
                                    it.fileSize
                                )
                            }
                        }
                        .toMap()

                    println("Loaded ${parityBlocksData.size} potential blocks...")

                    val paritySetConstituents: List<ParitySetConstituents> =
                        paritySetsConstituentsBySetId[paritySet.paritySetId]
                            ?: throw IllegalStateException("Missing parity set constituents for ${paritySet.paritySetId}!")

                    if (paritySetConstituents.find {it.dataBlockHash == liveBlock.dataBlockHash} == null) {
                        throw IllegalStateException("missing data block hash ${liveBlock.dataBlockHash} in restore set?!")
                    }

                    val alignedBlocks: List<ByteArray> = paritySetConstituents
                        .filter {it.dataBlockHash != liveBlock.dataBlockHash}
                        .groupBy { it.indexInSet }
                        .mapValues { (_, candidates) -> candidates.firstNotNullOf { filesBlockData[it.dataBlockHash] } }
                        .entries.sortedBy { it.key }.map {it.value}.toList()

                    val parityBlockData = parityBlocksData[paritySet.parityPHash]
                        ?: throw IllegalStateException("Missing parity block data for ${paritySet.parityPHash}")

                    val restoredBlock = restoreBlock(listOf(parityBlockData) + alignedBlocks)
                    val restoredHash = Hash.digest(restoredBlock)
                    if (liveBlock.dataBlockHash == restoredHash.storeable) {
                        println("Successful restore of block $liveBlock!")
                    } else {
                        throw IllegalStateException("Failed restore of parityId ${paritySet.paritySetId}!")
                    }
                    return@restoreLiveBlock restoredHash.storeable to restoredBlock
                }

            throw IllegalStateException("Failed restoring block ${liveBlock.dataBlockHash}!")
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
        ByteArray(fileVersion.size.toInt()) // todo rewrite on-line to support >2GB files
    necessaryDataBlocks.forEach { blockDescriptor ->
        val restoredBlockData = checkNotNull(restoredBlocks[blockDescriptor.dataBlockHash])

        restoredBlockData
            .sliceArray(blockDescriptor.blockOffset until blockDescriptor.blockOffset + blockDescriptor.chunkSize)
            .copyInto(restoredFileData, blockDescriptor.fileOffset.toInt())
    }

    val restoredFileDigest = Hash.digest(restoredFileData)
    if (restoredFileDigest.storeable != fileVersion.hash) {
        throw IllegalStateException("Restored file does not have expected hash!")
    }

    newLocation.write(fileVersion.path, restoredFileData)
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
