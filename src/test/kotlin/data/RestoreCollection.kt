package data

import data.parity.restoreBlock
import data.repo.sql.FileSize
import data.repo.sql.StorageDeviceGUID
import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.CatalogueFileVersions
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileDataBlocks
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.parity.ParityBlocks
import data.repo.sql.parity.ParitySetFileDataBlockMapping
import data.repo.sql.parity.ParitySets
import data.repo.sql.storagemedia.StorageFileLocations
import data.repo.sql.storagemedia.StorageMedias
import data.repo.sql.storagemedia.StorageParityLocations
import data.storage.Hash
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import io.github.oshai.kotlinlogging.KLogger
import org.jetbrains.exposed.sql.JoinType
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction


data class FileVersion(val path: String, val hash: Hash, val size: FileSize)

data class FileDataBlocksToRestore(
    val fileOffset: Long,
    val chunkSize: Int,
    val dataBlockHash: Hash,
    val blockOffset: Int,
)

data class ParitySetDefinition(
    val paritySetId: Hash,
    val blockSize: Int,
    val parityPHash: Hash,
    val numDeviceBlocks: Int
)

data class ParitySetConstituents(
    val paritySet: ParitySetDefinition,
    val dataBlockHash: Hash,
    val indexInSet: Int,
)

data class ParityBlocksForRestore(
    val paritySetId: Hash,
    val hash: Hash,
    val file: ReadonlyFileSystem.File?
)

data class FileDataBlockForRestore(
    val paritySetId: Hash,
    val fileHash: Hash,
    val fileOffset: Long,
    val chunkSize: Int,
    val dataBlockHash: Hash,
    val blockOffset: Int,
    val storageDeviceGUID: StorageDeviceGUID,
    val filePath: String,
    val fileObject: ReadonlyFileSystem.File?
)

fun <StorageMedia> restoreCollection(
    collectionRepo: StoredRepo,
    storageDevices: Map<StorageMedia, ReadonlyFileSystem>,
    newLocation: WritableDeviceFileSystem,
    logger: KLogger
) {
    transaction(collectionRepo.db) {
        // fixme weird logic...
        val storageDeviceGUIDs =
            StorageMedias.selectAll()
                .associate { (it[StorageMedias.label].toInt() as StorageMedia) to StorageMedias.guid(it) }
        val storageDeviceByGuid = storageDevices.mapKeys { storageDeviceGUIDs[it.key]!! }

        (CatalogueFileVersions innerJoin FileRefs).selectAll().forEach { row ->
            restoreFile(
                storageDeviceByGuid,
                FileVersion(
                    row[CatalogueFileVersions.path],
                    CatalogueFileVersions.hash(row),
                    FileRefs.size(row)
                ),
                newLocation,
                logger
            )
        }
    }
}

fun restoreFile(
    storageDeviceByGuid: Map<StorageDeviceGUID, ReadonlyFileSystem>,
    fileVersion: FileVersion,
    newLocation: WritableDeviceFileSystem,
    logger: KLogger
) {
    val digest = newLocation.digest(fileVersion.path)
    if (digest != null) {
        logger.debug { "File exists: ${fileVersion.path}, checking digest..." }
    }

    if (digest == fileVersion.hash) {
        logger.debug { "File contents are the same!" }
        return
    }

    val storedPossibleLocations = StorageFileLocations.selectAll()
        .where { StorageFileLocations.hash eq fileVersion.hash.storeable }
        .associate { StorageFileLocations.storageMedia(it) to it[StorageFileLocations.path] }
    logger.debug { "Found ${storedPossibleLocations.size} possible stored locations!" }

    val usableLocations = storedPossibleLocations.mapNotNull { (storageDeviceGUID, storageDevicePath) ->
        val device = storageDeviceByGuid[storageDeviceGUID]
        if (device == null) {
            logger.error { "Device with guid $storageDeviceGUID missing! Can't use for restore." }
            return@mapNotNull null
        } else {
            if (device.existsWithHash(storageDevicePath, fileVersion.hash, logger)) {
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

    logger.info { "Backup file ${fileVersion.path} is not available, restoring via parity checks." }

    logger.debug("Finding out what data blocks we need to restore the file...")


    val necessaryDataBlocks: List<FileDataBlocksToRestore> =
        FileDataBlockMappings.selectAll()
            .where { FileDataBlockMappings.fileHash eq fileVersion.hash.storeable }
            .map {
                FileDataBlocksToRestore(
                    it[FileDataBlockMappings.fileOffset],
                    it[FileDataBlockMappings.chunkSize],
                    FileDataBlockMappings.dataBlockHash(it),
                    it[FileDataBlockMappings.blockOffset],
                )
            }

    logger.debug("Will restore file ${fileVersion.hash} with ${necessaryDataBlocks.size} data blocks.")

    logger.debug("Finding parity sets...")
    val paritySetsDefinitionForNecessaryDataBlocks: List<ParitySetDefinition> =
        (ParitySetFileDataBlockMapping innerJoin ParitySets).selectAll()
            .where(ParitySetFileDataBlockMapping.dataBlockHash.inList(necessaryDataBlocks.map { it.dataBlockHash.storeable }))
            .map {
                ParitySetDefinition(
                    ParitySets.hash(it),
                    it[ParitySets.blockSize],
                    ParitySets.parityPHash(it),
                    it[ParitySets.numDeviceBlocks]
                )
            }.distinct()

    paritySetsDefinitionForNecessaryDataBlocks.forEach { logger.debug { it } }

    logger.debug("Loading all parity sets data...")
    val paritySetsConstituents = (ParitySetFileDataBlockMapping innerJoin ParitySets).selectAll()
        .where(ParitySetFileDataBlockMapping.paritySetId.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId.storeable }))
        .map {
            ParitySetConstituents(
                ParitySetDefinition(
                    ParitySets.hash(it),
                    it[ParitySets.blockSize],
                    ParitySets.parityPHash(it),
                    it[ParitySets.numDeviceBlocks]
                ),
                ParitySetFileDataBlockMapping.dataBlockHash(it),
                it[ParitySetFileDataBlockMapping.indexInSet],
            )
        }

    paritySetsConstituents.forEach { logger.debug { it } }

    logger.debug("Loading parity blocks information...")
    val parityBlocksForRestore = (ParitySets.join(
        ParityBlocks,
        JoinType.INNER,
        onColumn = ParitySets.parityPHash,
        otherColumn = ParityBlocks.hash
    ) innerJoin StorageParityLocations)
        .selectAll()
        .where { ParitySets.hash.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId.storeable }) }
        .map {
            val storageDeviceGUID = StorageParityLocations.storageMedia(it)
            val storageDevice = storageDeviceByGuid[storageDeviceGUID]

            ParityBlocksForRestore(
                ParitySets.hash(it),
                ParityBlocks.hash(it),
                storageDevice?.resolve(".repo/parity_blocks/${it[ParityBlocks.hash]}.parity")
            )
        }
    parityBlocksForRestore.forEach { logger.debug { it } }

    logger.debug("Loading file blocks information necessary for restore")
    val fileDataBlocksForRestore =
        (ParitySetFileDataBlockMapping innerJoin FileDataBlocks innerJoin FileDataBlockMappings innerJoin FileRefs
                innerJoin StorageFileLocations)
            .selectAll()
            .where(ParitySetFileDataBlockMapping.paritySetId.inList(paritySetsDefinitionForNecessaryDataBlocks.map { it.paritySetId.storeable }))
            .map {
                val storageDeviceGUID = StorageFileLocations.storageMedia(it)
                val storageDevice = storageDeviceByGuid[storageDeviceGUID]
                val storageLocationFilePath = it[StorageFileLocations.path]

                FileDataBlockForRestore(
                    ParitySetFileDataBlockMapping.paritySetId(it),
                    FileDataBlockMappings.fileHash(it),
                    it[FileDataBlockMappings.fileOffset],
                    it[FileDataBlockMappings.chunkSize],
                    FileDataBlockMappings.dataBlockHash(it),
                    it[FileDataBlockMappings.blockOffset],
                    StorageFileLocations.storageMedia(it),
                    storageLocationFilePath,
                    storageDevice?.resolve(storageLocationFilePath)
                )
            }

    fileDataBlocksForRestore.forEach { logger.debug { it } }

    val paritySetsPerBlockHash = paritySetsConstituents
        .groupBy { it.dataBlockHash }
        .mapValues { (_, constituents) -> constituents.map { it.paritySet }.distinct() }

    val paritySetsConstituentsBySetId: Map<Hash, List<ParitySetConstituents>> =
        paritySetsConstituents.groupBy { it.paritySet.paritySetId }
    val fileDataBlocksForRestorePerParitySetId = fileDataBlocksForRestore.groupBy { it.paritySetId }
    val parityBlocksForRestorePerParitySetId = parityBlocksForRestore.groupBy { it.paritySetId }

    val restoredBlocks: Map<Hash, FullSetBlockData> =
        necessaryDataBlocks.associate { liveBlock ->
            val setsToUseForBlock: List<ParitySetDefinition> =
                paritySetsPerBlockHash[liveBlock.dataBlockHash]
                    ?: throw IllegalStateException("Cannot restore block ${liveBlock.dataBlockHash}, no parity set to use!")

            liveBlock.dataBlockHash to restoreLiveBlock(
                setsToUseForBlock,
                fileDataBlocksForRestorePerParitySetId,
                parityBlocksForRestorePerParitySetId,
                paritySetsConstituentsBySetId,
                logger
            )
                .apply { check(liveBlock.dataBlockHash in this) { "missing data block hash ${liveBlock.dataBlockHash} in restore set?!" } }
        }

    writeRestoredFile(necessaryDataBlocks, restoredBlocks, logger, fileVersion, newLocation)
}

private fun writeRestoredFile(
    necessaryDataBlocks: List<FileDataBlocksToRestore>,
    restoredBlocks: Map<Hash, FullSetBlockData>,
    logger: KLogger,
    fileVersion: FileVersion,
    newLocation: WritableDeviceFileSystem
) {
    val unrecoverableBlocks = necessaryDataBlocks.filter { it.dataBlockHash !in restoredBlocks.keys }
    unrecoverableBlocks.forEach {
        logger.error { "Failed restoring block $it" }
    }

    val necessaryDataBlockHashes = necessaryDataBlocks.map { it.dataBlockHash }
    restoredBlocks.keys.filter { it !in necessaryDataBlockHashes }.forEach {
        logger.error { "Restored unnecessary block $it, but it is fine." }
    }

    val restoredFileData =
        ByteArray(fileVersion.size.toInt()) // todo rewrite on-line to support >2GB files
    necessaryDataBlocks.forEach { blockDescriptor ->
        val restoredSetBlockData = checkNotNull(restoredBlocks[blockDescriptor.dataBlockHash])
        val restoredBlockData = restoredSetBlockData[blockDescriptor.dataBlockHash]

        restoredBlockData
            .sliceArray(blockDescriptor.blockOffset until blockDescriptor.blockOffset + blockDescriptor.chunkSize)
            .copyInto(restoredFileData, blockDescriptor.fileOffset.toInt())
    }

    val restoredFileDigest = Hash.digest(restoredFileData)
    if (restoredFileDigest != fileVersion.hash) {
        throw IllegalStateException("Restored file does not have expected hash!")
    }

    newLocation.write(fileVersion.path, restoredFileData)
}

class FullSetBlockData(val dataBlocks: Map<Hash, ByteArray>, val parityBlocks: Map<Hash, ByteArray>) {
    val blocksData: Map<Hash, ByteArray> = dataBlocks + parityBlocks

    operator fun get(dataBlockHash: Hash): ByteArray = checkNotNull(blocksData[dataBlockHash])
    operator fun contains(dataBlockHash: Hash): Boolean = dataBlockHash in blocksData
}

class PartialParitySet(
    val dataBlocks: Map<Hash, ByteArray?>,
    val dataBlockIdxs: List<Hash>,
    val parityBlocks: Map<Hash, ByteArray?>,
    val parityPHash: Hash,
    // todo add raid types
) {
    fun restore(logger: KLogger): FullSetBlockData {
        check(dataBlockIdxs.containsAll(dataBlocks.keys)) { "bad indexes, ${dataBlocks.keys} !in $dataBlockIdxs!" }
        val providedParityBlockData = parityBlocks[parityPHash]
        val providedFileBlocksData = dataBlockIdxs.map { dataBlocks[it] }

        val missingBlocksCnt = providedFileBlocksData.count { it == null }

        return if (providedParityBlockData != null && missingBlocksCnt == 0) {
            logger.info { "All blocks with data available! Returning ..." }
            FullSetBlockData(
                dataBlocks.mapValues { checkNotNull(it.value) },
                parityBlocks.mapValues { checkNotNull(it.value) })
        } else if (providedParityBlockData == null && missingBlocksCnt == 0) {
            check(missingBlocksCnt == 0)
            logger.info { "Parity block missing, restoring ..." }

            // restore parity block
            val parityBlockData = restoreBlock(providedFileBlocksData.map { checkNotNull(it) })
            FullSetBlockData(
                dataBlocks.mapValues { checkNotNull(it.value) },
                parityBlocks.mapValues {
                    if (it.key == parityPHash) {
                        parityBlockData
                    } else checkNotNull(it.value)
                })
        } else if (providedParityBlockData != null && missingBlocksCnt == 1) {
            val blockWithoutData: Hash = dataBlockIdxs[providedFileBlocksData.indexOfFirst { it == null }]
            logger.debug { "Restoring data block $blockWithoutData ..." }

            // restore file data block
            val fileBlockData = restoreBlock(listOf(providedParityBlockData) + providedFileBlocksData.filterNotNull())
                .apply {
                    check(Hash.digest(this) == blockWithoutData) {
                        "Failed restore $blockWithoutData, got ${Hash.digest(this)} instead!"
                    }
                }
            FullSetBlockData(
                dataBlocks.mapValues {
                    if (it.key == blockWithoutData) {
                        fileBlockData
                    } else checkNotNull(it.value)
                },
                parityBlocks.mapValues {
                    checkNotNull(it.value)
                })
        } else {
            logger.error { "Unsupported case - providedParityBlockData = $providedParityBlockData and missingBlocksCnt = $missingBlocksCnt!" }
            throw IllegalStateException("Unsupported case - providedParityBlockData = $providedParityBlockData and missingBlocksCnt = $missingBlocksCnt!")
        }
    }
}

private fun restoreLiveBlock(
    setsToUseForBlock: List<ParitySetDefinition>,
    fileDataBlocksForRestorePerParitySetId: Map<Hash, List<FileDataBlockForRestore>>,
    parityBlocksForRestorePerParitySetId: Map<Hash, List<ParityBlocksForRestore>>,
    paritySetsConstituentsBySetId: Map<Hash, List<ParitySetConstituents>>,
    logger: KLogger
): FullSetBlockData {
    logger.debug("Trying restore with ${setsToUseForBlock.size} potential parity blocks...")
    for (paritySet in setsToUseForBlock) {
        logger.debug("Restoring by ${paritySet.paritySetId} with block size ${paritySet.blockSize}")

        val fileDataBlocks =
            fileDataBlocksForRestorePerParitySetId[paritySet.paritySetId] ?: continue
        logger.debug("Restoring by ${paritySet.paritySetId} with ${fileDataBlocks.size} potential file blocks...")

        val filesBlockData: Map<Hash, ByteArray> = fileDataBlocks.groupBy { it.dataBlockHash }
            .mapNotNull { (dataBlockHash, filesForBlockMultiDevices) ->
                logger.debug("Restoring with $dataBlockHash with ${filesForBlockMultiDevices.size} potential blocks...")
                filesForBlockMultiDevices.forEach { logger.debug { it } }

                filesForBlockMultiDevices.groupBy { it.storageDeviceGUID }
                    .mapNotNull {
                        logger.debug("Using $it.key with ${it.value.size} files to restore...")
                        it.value.forEach { logger.debug { it } }

                        loadFileBlockFromDevice(paritySet, dataBlockHash, it.value, logger)
                    }.firstOrNull()
            }.toMap()

        val parityBlocks = parityBlocksForRestorePerParitySetId[paritySet.paritySetId] ?: continue
        logger.debug("Restoring by ${paritySet.paritySetId} with ${fileDataBlocks.size} potential parity blocks...")

        val parityBlocksData = parityBlocks.associate { parityBlock ->
            parityBlock.hash to parityBlock.file?.let { it.dataInRange(0, it.fileSize) }
        }
        logger.debug("Loaded ${parityBlocksData.size} potential blocks...")

        val paritySetConstituents: List<ParitySetConstituents> =
            paritySetsConstituentsBySetId[paritySet.paritySetId]
                ?: throw IllegalStateException("Missing parity set constituents for ${paritySet.paritySetId}!")

        val hashAlignedToIdx: List<Hash> =
            paritySetConstituents
                .associate { it.indexInSet to it.dataBlockHash }
                .entries.sortedBy { it.key }.map { it.value }

        val alignedBlocks: Map<Hash, ByteArray?> = paritySetConstituents
            .groupBy { it.dataBlockHash }
            .mapValues { (_, candidates) -> candidates.firstNotNullOfOrNull { filesBlockData[it.dataBlockHash] } }

        if (alignedBlocks.values.all { it != null } && parityBlocksData.values.all { it != null }) {
            logger.info { "File block is actually available! Returning..." }
            return FullSetBlockData(
                alignedBlocks.mapValues { checkNotNull(it.value) },
                parityBlocksData.mapValues { checkNotNull(it.value) })
        }
        return PartialParitySet(
            alignedBlocks,
            hashAlignedToIdx,
            parityBlocksData,
            paritySet.parityPHash
        ).restore(logger)
    }

    throw IllegalStateException("Failed restoring!")
}

private fun loadFileBlockFromDevice(
    paritySet: ParitySetDefinition,
    dataBlockHash: Hash,
    filesForBlock: List<FileDataBlockForRestore>,
    logger: KLogger
): Pair<Hash, ByteArray>? {
    val blockData = ByteArray(paritySet.blockSize)
    for (it in filesForBlock) {
        if (it.fileObject == null) {
            logger.debug("missing file ${it.filePath} on ${it.storageDeviceGUID}")
            return null
        }

        val fileData = it.fileObject.dataInRange(it.fileOffset, it.fileOffset + it.chunkSize)
        fileData.copyInto(blockData, it.blockOffset)
    }
    val restoredBlockHash = Hash.digest(blockData)
    check(restoredBlockHash == dataBlockHash) {
        "Restored digest $restoredBlockHash != $dataBlockHash!"
    }
    return restoredBlockHash to blockData
}


fun ReadonlyFileSystem.existsWithHash(
    path: String,
    fileHash: Hash,
    logger: KLogger
): Boolean {
    val existingDigest = this.digest(path)
    return if (existingDigest == null) {
        false
    } else {
        logger.debug("Found file to restore from... checking hash")
        if (existingDigest != fileHash) {
            logger.error("BAD HASH for $path!")
            false
        } else {
            logger.debug("Hash is correct! Using...")
            true
        }
    }
}