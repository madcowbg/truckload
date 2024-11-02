package processes

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
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction


data class FileVersion(val path: String, val hash: Hash, val size: FileSize)

data class FileDataBlocksToRestore(
    val fileOffset: Long,
    val chunkSize: Int,
    val dataBlockHash: Hash,
    val blockOffset: Int,
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
                StorageCatalogue(storageDeviceByGuid),
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
    storageCatalogue: StorageCatalogue,
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
        val device = storageCatalogue[storageDeviceGUID]
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

    restoreFile(fileVersion, storageCatalogue, newLocation, logger)
}

private fun restoreFile(
    fileVersion: FileVersion,
    storageCatalogue: StorageCatalogue,
    newLocation: WritableDeviceFileSystem,
    logger: KLogger
) {
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

    val restoredBlocks: Map<Hash, FullSetBlockData> =
        necessaryDataBlocks.associate { liveBlock ->
            liveBlock.dataBlockHash to restoreLiveBlock(liveBlock.dataBlockHash, storageCatalogue, logger)
                .apply {
                    check(liveBlock.dataBlockHash in this) { "missing data block hash ${liveBlock.dataBlockHash} in restore set?!" }
                }
        }

    writeRestoredFile(necessaryDataBlocks, restoredBlocks, logger, fileVersion, newLocation)
}

class StorageCatalogue(private val availableStorage: Map<StorageDeviceGUID, ReadonlyFileSystem>) {
    operator fun get(storageDeviceGUID: StorageDeviceGUID): ReadonlyFileSystem? = availableStorage[storageDeviceGUID]

    fun findFileIfExists(
        storageDeviceGUID: StorageDeviceGUID,
        storageLocationFilePath: String,
        logger: KLogger
    ): ReadonlyFileSystem.File? {
        val storageDevice = this[storageDeviceGUID]
        val file = storageDevice?.resolve(storageLocationFilePath)

        if (file == null) {
            logger.debug { "missing file $storageLocationFilePath from $storageDeviceGUID!" }
        }
        return file
    }
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

class BlockToStorageToChunks(
    val dataBlockHash: Hash,
    val storageMedia: StorageDeviceGUID,
    val fileChunksForRestore: FileChunksForRestore
)

private fun restoreLiveBlock(
    blockToRestore: Hash,
    storageCatalogue: StorageCatalogue,
    logger: KLogger
): FullSetBlockData {
    logger.debug("Trying block $blockToRestore...")
    val query = (ParitySetFileDataBlockMapping innerJoin ParitySets).selectAll()
        .where(ParitySetFileDataBlockMapping.dataBlockHash.eq(blockToRestore.storeable))
    for (row in query) {
        val paritySetId = ParitySets.hash(row)
        val blockSize = ParitySets.blockSize(row)
        val parityPHash = ParitySets.parityPHash(row)
        logger.debug("Restoring by $paritySetId with block size $blockSize")

        val filesBlockData: Map<Hash, ByteArray> =
            loadExistingFileBlockData(paritySetId, storageCatalogue, logger, blockSize)
        logger.debug { "Restoring by $paritySetId with ${filesBlockData.size} potential file blocks..." }

        logger.debug("Loading parity blocks information...")

        val parityBlocksData = loadParityBlocksData(paritySetId, storageCatalogue, logger)
        logger.debug { "Loaded ${parityBlocksData.size} potential blocks..." }

        val dataBlockIdxs: List<Hash> = ParitySetFileDataBlockMapping.selectAll()
            .where(ParitySetFileDataBlockMapping.paritySetId.eq(paritySetId.storeable))
            .orderBy(ParitySetFileDataBlockMapping.indexInSet)
            .map { ParitySetFileDataBlockMapping.dataBlockHash(it) }

        val dataBlocks: Map<Hash, ByteArray?> = dataBlockIdxs.associateWith { filesBlockData[it] }

        return PartialParitySet(dataBlocks, dataBlockIdxs, parityBlocksData, parityPHash).restore(logger)
    }

    throw IllegalStateException("Failed restoring!")
}

private fun loadParityBlocksData(paritySetId: Hash, storageCatalogue: StorageCatalogue, logger: KLogger) =
    (ParitySets.join(
        ParityBlocks,
        JoinType.INNER,
        onColumn = ParitySets.parityPHash,
        otherColumn = ParityBlocks.hash
    ) innerJoin StorageParityLocations)
        .selectAll()
        .where { ParitySets.hash.eq(paritySetId.storeable) }
        .associate { it ->
            ParityBlocks.hash(it) to storageCatalogue.findFileIfExists(
                StorageParityLocations.storageMedia(it),
                ".repo/parity_blocks/${it[ParityBlocks.hash]}.parity",
                logger
            )?.let { it.dataInRange(0, it.fileSize) }
        }

private fun loadExistingFileBlockData(
    paritySetId: Hash,
    storageCatalogue: StorageCatalogue,
    logger: KLogger,
    blockSize: Int
) = loadSetToStorageToChunkMap(paritySetId, storageCatalogue, logger)
    .mapNotNull { (dataBlockHash, filesForBlockMultiDevices) ->
        loadBlockFromLiveFile(dataBlockHash, blockSize, filesForBlockMultiDevices, logger)
    }.toMap()

private fun loadSetToStorageToChunkMap(
    paritySetId: Hash,
    storageCatalogue: StorageCatalogue,
    logger: KLogger
): Map<Hash, Map<StorageDeviceGUID, List<FileChunksForRestore>>> = (ParitySetFileDataBlockMapping
        innerJoin FileDataBlocks innerJoin FileDataBlockMappings innerJoin FileRefs innerJoin StorageFileLocations)
    .selectAll()
    .where(ParitySetFileDataBlockMapping.paritySetId.eq(paritySetId.storeable))
    .map {
        val file = storageCatalogue
            .findFileIfExists(
                StorageFileLocations.storageMedia(it),
                it[StorageFileLocations.path],
                logger
            )
        BlockToStorageToChunks(
            FileDataBlockMappings.dataBlockHash(it),
            StorageFileLocations.storageMedia(it),
            FileChunksForRestore(
                file,
                it[FileDataBlockMappings.fileOffset],
                it[FileDataBlockMappings.chunkSize],
                it[FileDataBlockMappings.blockOffset]
            )
        )
    }
    .groupBy { it.dataBlockHash }
    .mapValues { (_, value) ->
        value.groupBy { it.storageMedia }
            .mapValues { (_, value) -> value.map { it.fileChunksForRestore } }
    }

private fun loadBlockFromLiveFile(
    dataBlockHash: Hash,
    blockSize: Int,
    filesForBlockMultiDevices: Map<StorageDeviceGUID, List<FileChunksForRestore>>,
    logger: KLogger
): Pair<Hash, ByteArray>? {
    logger.info("Restoring with $dataBlockHash with ${filesForBlockMultiDevices.size} potential blocks...")
    filesForBlockMultiDevices.forEach { logger.info { it } }

    return filesForBlockMultiDevices.entries.firstNotNullOfOrNull {
        logger.debug { "Using ${it.key} with ${it.value.size} files to restore..." }
        it.value.forEach { logger.debug { it } }

        loadFileBlockFromDevice(dataBlockHash, blockSize, it.value, logger)
    }
}

data class FileChunksForRestore(
    val fileObject: ReadonlyFileSystem.File?,
    val fileOffset: Long,
    val chunkSize: Int,
    val blockOffset: Int
)

private fun loadFileBlockFromDevice(
    dataBlockHash: Hash,
    blockSize: Int,
    filesForBlock: List<FileChunksForRestore>,
    logger: KLogger
): Pair<Hash, ByteArray>? {
    val blockData = ByteArray(blockSize)
    for (it in filesForBlock) {
        if (it.fileObject == null) {
            logger.debug { "missing file!" }
            return null
        }

        val fileData = it.fileObject.dataInRange(it.fileOffset, it.fileOffset + it.chunkSize)
        fileData.copyInto(blockData, it.blockOffset)
    }
    val restoredBlockHash = Hash.digest(blockData)
    check(restoredBlockHash == dataBlockHash) { "Restored digest $restoredBlockHash != $dataBlockHash!" }
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