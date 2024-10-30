package data

import data.parity.ParitySet
import data.parity.naiveBlockMapping
import data.repo.sql.*
import data.repo.sql.storagemedia.StorageFileLocations
import data.repo.sql.storagemedia.StorageParityLocations
import data.repo.sql.storagemedia.StorageMedias
import data.storage.LiveBlock
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.math.abs
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
fun <StorageMedia> storeCollectionBackup(
    collectionRepo: StoredRepo,
    currentCollectionFilesLocation: ReadonlyFileSystem,
    storageDevices: Map<StorageMedia, WritableDeviceFileSystem>
) {
    val numPartitions = storageDevices.size

    println("Reading folder ...")
    val storedFiles = currentCollectionFilesLocation.walk()
    println("insert files in catalogue...")
    insertFilesInCatalogue(collectionRepo, storedFiles)

    val partitionedFiles = storedFiles.groupBy { (abs(it.hash.hashCode()) % numPartitions) as StorageMedia }

    println("Mapping to blocks ...")
    val blockSize = 1 shl 12 // 4KB
    val blockMappings = partitionedFiles.mapValues { naiveBlockMapping(it.value.iterator(), blockSize = blockSize) }

    println("insert live block mapping...")
    blockMappings.forEach { insertLiveBlockMapping(collectionRepo, it.value) }

    println("Calculating parity sets ...")
    val paritySets = matchingParitySets(blockMappings)

    paritySets.forEach { insertComputedParitySets(collectionRepo, it.value) }

    val deviceGuids: Map<StorageMedia, String> = transaction(collectionRepo.db) {
        storageDevices.mapValues { device ->
            val deviceGuid = Uuid.random().toString()
            StorageMedias.insert {
                it[guid] = deviceGuid
                it[label] = device.key.toString()
                it[totalSize] = 0
                it[freeSize] = 0
            }
            return@mapValues deviceGuid
        }
    }

    println("saving parity sets...")
    transaction(collectionRepo.db) {
        paritySets.forEach { (device, paritySetsForDevice) ->
            val storageDevice = checkNotNull(storageDevices[device])
            val storagePath = storageDevice.root.resolve(".repo/")
            storagePath.mkdirs()
            storeParitySets(storagePath.toPath(), paritySetsForDevice)

            val deviceGuid: String = checkNotNull(deviceGuids[device]) { "invalid device: $device" }
            paritySetsForDevice.forEach { paritySet ->
                StorageParityLocations.insert {
                    it[hash] = paritySet.parityBlock.hash.storeable
                    it[storageMedia] = deviceGuid
                }
            }
        }
    }

    println("saving split files...")
    transaction(collectionRepo.db) {
        partitionedFiles.forEach { (deviceId, files) ->
            val deviceGuid: String = checkNotNull(deviceGuids[deviceId]) { "invalid device: $deviceId" }

            val storageDevice = checkNotNull(storageDevices[deviceId])
            files.forEach { file ->
                storageDevice.copy(file, file.path) // copy physical file to same path

                StorageFileLocations.insert {
                    it[storageMedia] = deviceGuid
                    it[path] = file.path
                    it[hash] = file.hash.storeable
                }
            }
        }
    }

    println("done init!")
}

fun <StorageMedia> matchingParitySets(blockMapping: Map<StorageMedia, List<LiveBlock>>): Map<StorageMedia, List<ParitySet>> {
    val liveBlockMatchingIterators = blockMapping.mapValues { it.value.iterator() }

    val parityDestinationStorageMedias: List<StorageMedia> = blockMapping.keys.toList()
    var parityDestinationIdx = 0 // rotate media...
    val paritySets: MutableMap<StorageMedia, MutableList<ParitySet>> = mutableMapOf()
    while (liveBlockMatchingIterators.any { it.value.hasNext() }) {
        val matchedLiveBlocks = liveBlockMatchingIterators.mapNotNull {
            if (it.key == parityDestinationStorageMedias[parityDestinationIdx]) {
                // skip, as we store parity here
                null
            } else if (it.value.hasNext()) {
                it.value.next()
            } else null
        }

        if (matchedLiveBlocks.isNotEmpty()) {
            check(matchedLiveBlocks.distinctBy { it.size }.size == 1) { "have more than 1 parity block size!" }

            paritySets
                .computeIfAbsent(parityDestinationStorageMedias[parityDestinationIdx]) { mutableListOf() }
                .add(ParitySet(liveBlocks = matchedLiveBlocks))
        } else {
            println("Skipping iteration as all live blocks are empty...")
        }
        parityDestinationIdx = (parityDestinationIdx + 1) % parityDestinationStorageMedias.size
    }

    return paritySets
}
