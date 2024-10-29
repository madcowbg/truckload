package data

import data.parity.ParitySet
import data.parity.naiveBlockMapping
import data.repo.sql.*
import data.storage.DeviceFileSystem
import data.storage.LiveBlock
import data.storage.ReadonlyFileSystem
import data.storage.WritableDeviceFileSystem
import org.junit.jupiter.api.Test
import perftest.TestDataSettings
import kotlin.math.abs

class CreateCollectionTest {

    @Test
    fun `create repo and split into storage media`() {
        val testPath = "${TestDataSettings.test_path}/.experiments/test_collection/"
        val repoPath = "$testPath.repo"
        StoredRepo.delete(repoPath)
        StoredRepo.init(repoPath)
        val collectionRepo: StoredRepo = StoredRepo.connect(repoPath)

        val currentCollectionFilesLocation: ReadonlyFileSystem =
            DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        println("Reading folder ...")
        val storedFiles = currentCollectionFilesLocation.walk()
        println("insert files in catalogue...")
        insertFilesInCatalogue(collectionRepo, storedFiles)

        val numPartitions = 6
        val partitionedFiles = storedFiles.groupBy { abs(it.hash.hashCode()) % numPartitions }

        println("Mapping to blocks ...")
        val blockSize = 1 shl 12 // 4KB
        val blockMappings = partitionedFiles.mapValues { naiveBlockMapping(it.value.iterator(), blockSize = blockSize) }

        println("insert live block mapping...")
        blockMappings.forEach { insertLiveBlockMapping(collectionRepo, it.value) }

        println("Calculating parity sets ...")
        val paritySets = matchingParitySets(blockMappings)

        paritySets.forEach { insertComputedParitySets(collectionRepo, it.value) }

        val storageDevices = partitionedFiles.mapValues { WritableDeviceFileSystem("$testPath/Device ${it.key}/") }

        println("saving parity sets...")
        paritySets.forEach {
            val storageDevice = checkNotNull(storageDevices[it.key])
            val storagePath = storageDevice.root.resolve(".repo/")
            storagePath.mkdirs()
            storeParitySets(storagePath.toPath(), it.value)
        }

        println("saving split files...")
        partitionedFiles.forEach { partition ->
            val storageDevice = checkNotNull(storageDevices[partition.key])
            partition.value.forEach {
                storageDevice.copy(it, it.path) // copy physical file to same path
            }
        }

        println("done init!")
    }
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
