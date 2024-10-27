package data.repo.sql

import data.parity.BlockMapping
import data.parity.naiveBlockMapping
import data.parity.naiveParitySets
import data.repo.Repo
import data.repo.readFolder
import data.storage.DeviceFileSystem
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.FileOutputStream
import java.nio.file.Path
import kotlin.io.path.*

class StoredRepo private constructor(val db: Database, val rootFolder: Path) {

    companion object {
        @OptIn(ExperimentalPathApi::class)
        fun delete(repoPath: String) {
            if (Path(repoPath).exists()) {
                Path(repoPath).deleteRecursively()
            }
        }

        fun init(repoPath: String): StoredRepo {
            Path(repoPath).createDirectories()

            val repo = connect(repoPath)
            transaction(repo.db) {
                SchemaUtils.create(DataBlocks, FileRefs, FileDataBlockMappings, CatalogueFile, ParityBlocks, ParitySets, ParityDataBlockMappings)
            }

            val parityBlocksPath = "$repoPath/parity_blocks"
            Path(parityBlocksPath).createDirectories()

            return repo
        }

        fun connect(repoPath: String): StoredRepo = StoredRepo(
            Database.connect("jdbc:sqlite:$repoPath/repo.db?foreign_keys=on;", "org.sqlite.JDBC"),
            Path(repoPath)
        )
    }
}

class InvalidRepoData(val message: String)

fun StoredRepo.naiveInitializeRepo(location: DeviceFileSystem): StoredRepo {
    val storedRepo = this

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
    return storedRepo
}
