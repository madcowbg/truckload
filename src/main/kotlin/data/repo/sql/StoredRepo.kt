package data.repo.sql

import data.parity.ParitySet
import data.parity.naiveBlockMapping
import data.parity.naiveParitySets
import data.repo.sql.catalogue.FileVersions
import data.repo.sql.catalogue.VersionState
import data.repo.sql.datablocks.DataBlocks
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.parity.ParityBlocks
import data.repo.sql.parity.ParityDataBlockMappings
import data.repo.sql.parity.ParitySets
import data.repo.sql.parity.ParityType
import data.repo.sql.storagemedia.ParityLocations
import data.repo.sql.storagemedia.StorageMedia
import data.repo.sql.storagemedia.FileLocations
import data.storage.ReadonlyFileSystem
import data.storage.Hash
import data.storage.LiveBlock
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.FileOutputStream
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.sequences.Sequence

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
                SchemaUtils.create(
                    DataBlocks,
                    FileRefs,
                    FileDataBlockMappings,
                    ParityBlocks,
                    ParitySets,
                    ParityDataBlockMappings,
                    FileVersions,
                    FileLocations,
                    ParityLocations,
                    StorageMedia
                )
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

fun StoredRepo.naiveInitializeRepo(location: ReadonlyFileSystem, logger: (String) -> Unit): StoredRepo {
    logger("Reading folder ...")
    val storedFiles = location.walk()
    logger("Mapping to blocks ...")
    val blockMapping: List<LiveBlock> = naiveBlockMapping(storedFiles.iterator())
    logger("Calculating parity sets ...")
    val paritySets = naiveParitySets(blockMapping)

    logger("insert files in catalogue...")
    insertFilesInCatalogue(this, storedFiles)

    logger("insert live block mapping...")
    insertLiveBlockMapping(this, blockMapping)

    logger("insert computed parity sets...")
    insertComputedParitySetsAndStore(this, paritySets)

    logger("done init!")
    return this
}

fun insertFilesInCatalogue(storedRepo: StoredRepo, storedFiles: Sequence<ReadonlyFileSystem.File>) {
    transaction(storedRepo.db) {
        for (file in storedFiles) {
            FileRefs.insertIgnore { // ignore because two files can be in different places
                it[hash] = file.hash.storeable
                it[size] = file.fileSize
            }

            FileVersions.insert {
                it[path] = file.path
                it[hash] = file.hash.storeable
                it[state] = VersionState.EXISTING
            }
        }
    }
}

fun insertLiveBlockMapping(storedRepo: StoredRepo, blockMapping: List<LiveBlock>) {
    // insert live block mapping
    transaction(storedRepo.db) {
        for (liveBlock in blockMapping) {
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
}

fun insertComputedParitySetsAndStore(storedRepo: StoredRepo, paritySets: List<ParitySet>) {
    insertComputedParitySets(storedRepo, paritySets)
    storeParitySets(storedRepo.rootFolder, paritySets)
}

fun insertComputedParitySets(storedRepo: StoredRepo, paritySets: List<ParitySet>) {
    transaction(storedRepo.db) {
        for (paritySet in paritySets) {
            val parityBlock = paritySet.parityBlock
            ParityBlocks.insertIgnore {
                it[hash] = parityBlock.hash.storeable
                it[size] = parityBlock.size
            }

            val paritySetHash = Hash.digest(
                (paritySet.liveBlocks.map { it.hash } + listOf(paritySet.parityBlock.hash))
                    .joinToString("|")
                    .toByteArray()
            )

            // define the parity set
            ParitySets.insertIgnore {
                it[hash] = paritySetHash.storeable
                it[numDeviceBlocks] = paritySet.liveBlocks.size
                it[parityPHash] = paritySet.parityBlock.hash.storeable
                it[parityType] = ParityType.RAID5
            }

            for (idx in paritySet.liveBlocks.indices) {
                val liveBlock = paritySet.liveBlocks[idx]
                ParityDataBlockMappings.insertIgnore {
                    it[paritySetId] = paritySetHash.storeable
                    it[indexInSet] = idx
                    it[dataBlockHash] = liveBlock.hash.storeable
                }
            }
        }
    }
}

fun storeParitySets(rootFolder: Path, paritySets: List<ParitySet>) {
    for (paritySet in paritySets) {
        val parityBlock = paritySet.parityBlock

        // create parity blocks folder (if missing...)
        val parityBlocksPath = rootFolder.resolve("parity_blocks/")
        parityBlocksPath.toFile().mkdirs()

        val parityBlockFile = parityBlocksPath.resolve(parityBlock.hash.storeable + ".parity")
        if (!parityBlockFile.exists()) {
            val outputStream = FileOutputStream(parityBlockFile.toFile())
            outputStream.write(parityBlock.data)
            outputStream.close()
        } else {
            println("block file $parityBlockFile already exists") // fixme at least check digests maybe?
        }
    }
}

