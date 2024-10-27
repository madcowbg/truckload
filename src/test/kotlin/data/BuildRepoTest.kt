package data

import data.parity.BlockMapping
import data.parity.naiveBlockMapping
import data.parity.naiveParitySets
import data.repo.Repo
import data.repo.readFolder
import data.repo.sql.*
import data.storage.DeviceFileSystem
import data.storage.ParityBlock
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.insertIgnore
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.FileOutputStream
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.io.path.exists
import kotlin.io.path.writer
import kotlin.test.Test

@OptIn(ExperimentalEncodingApi::class)
class BuildRepoTest {

    @Test
    fun `read folder on hd and create repo`() {

        val repoPath = "${TestDataSettings.test_path}/.experiments/test_build/.repo"
        StoredRepo.delete(repoPath)
        val storedRepo: StoredRepo = StoredRepo.init(repoPath)

        val location = DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        val repo: Repo = readFolder(location)
        val blockMapping: BlockMapping = naiveBlockMapping(repo)
        val paritySets = naiveParitySets(blockMapping)

        // insert files in catalogue
        for (file in repo.storage) {
            transaction(storedRepo.db) {
                FileRefs.insertIgnore { // ignore because two files can be in different places
                    it[hash] = file.hash.storeable
                    it[size] = file.size
                }

                CatalogueFile.insert {
                    it[path] = file.path
                    it[hash] = file.hash.storeable
                }
            }
        }

        // insert live block mapping
        for (liveBlock in blockMapping.fileBlocks) {
            transaction(storedRepo.db) {
                if (false && !DataBlocks.selectAll().where(DataBlocks.hash.eq(liveBlock.hash.storeable)).empty()) {
                    println(
                        "duplicate parity block ${liveBlock.hash} " +
                                "when # of stored is ${DataBlocks.selectAll().count()}."
                    )
                }

                DataBlocks.insertIgnore { // blocks can duplicate, e.g. empty or repeating values
                    it[hash] = liveBlock.hash.storeable
                    it[size] = liveBlock.size
                }

                for (file in liveBlock.files) {
                    FileDataBlockMappings.insertIgnore { // files with the same content will duplicate
                        it[dataBlockHash] = liveBlock.hash.storeable
                        it[blockOffset] = 0 // fixme this is the naive way to store - one file at most per block...
                        it[fileOffset] = file.from
                        it[chunkSize] = file.size
                        it[fileHash] = file.fileHash.storeable
                    }
                }

            }
        }

        // insert computed parity sets
        transaction(storedRepo.db) {
            for (paritySet in paritySets) {
                val parityBlock = paritySet.parityBlock
                ParityBlocks.insertIgnore {
                    it[hash] = parityBlock.hash.storeable
                    it[size] = parityBlock.size
                }

                // create parity blocks folder (if missing...)
                val parityBlocksPath = storedRepo.rootFolder.resolve("parity_blocks/")
                parityBlocksPath.toFile().mkdirs()

                val parityBlockFile = parityBlocksPath.resolve(parityBlock.hash.storeable + ".parity")
                if (!parityBlockFile.exists()) {
                    val outputStream = FileOutputStream(parityBlockFile.toFile())
                    outputStream.write(parityBlock.data)
                    outputStream.close()
                } else {
                    println("block file $parityBlockFile already exists") // fixme at least check digests maybe?
                }

                // define the parity set
                val paritySetId = ParitySets.insertAndGetId {
                    it[numDeviceBlocks] = paritySet.liveBlocks.size
                    it[parityPHash] = paritySet.parityBlock.hash.storeable
                    it[parityType] = ParityType.RAID5
                }

                for (idx in paritySet.liveBlocks.indices) {
                    val liveBlock = paritySet.liveBlocks[idx]
                    ParityDataBlockMappings.insert {
                        it[ParityDataBlockMappings.paritySetId] = paritySetId
                        it[indexInSet] = idx
                        it[dataBlockHash] = liveBlock.hash.storeable
                    }
                }
            }
        }

        storedRepo.listOfIssues().forEach { println(it.message) }
        transaction(storedRepo.db) {
            CatalogueFile.selectAll().forEach { println(it[CatalogueFile.path]) }
        }
    }
}