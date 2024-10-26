package data.repo

import data.repo.FileChunkRefs.check
import data.repo.FileRefs.check
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails


object ParityBlocks : Table("parity_blocks") {
    val parityHash = text("parity_hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(parityHash)
}

object FileRefs : Table("file_refs") {
    val fileHash = text("file_hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(fileHash)
}

object FileChunkRefs : Table("file_chunk_ref") {
    val fileHash = reference("file_hash", FileRefs.fileHash)
    val fromByte = long("from_byte").check("fromByte_must_be_positive") { it.greaterEq(0) }
    val toByte = long("to_byte").check("fromByte_must_be_before_toByte") { it.greaterEq(fromByte) }
}

object ParityFileRefs : Table("parity_file_refs") {
    val parityBlock  = reference("parity_block_hash", ParityBlocks.parityHash, onDelete = ReferenceOption.RESTRICT)
    val fileChunk = reference("file_chunk_hash", FileChunkRefs.fileHash, onDelete = ReferenceOption.RESTRICT)
    val fromByte = long("from_byte").check("fromByte_must_be_positive") { it.greaterEq(0) }
    val toByte = long("to_byte").check("fromByte_must_be_before_toByte") { it.greaterEq(fromByte) }
}


class RepoData private constructor(val db: Database, val rootFolder: Path) {

    companion object {
        @OptIn(ExperimentalPathApi::class)
        fun delete(repoPath: String) {
            if (Path(repoPath).exists()) {
                Path(repoPath).deleteRecursively()
            }
        }

        fun init(repoPath: String): RepoData {
            Path(repoPath).createDirectories()

            val repo = connect(repoPath)
            transaction(repo.db) {
                SchemaUtils.create(ParityBlocks, FileRefs, FileChunkRefs, ParityFileRefs)
            }

            val parityBlocksPath = "$repoPath/parity_blocks"
            Path(parityBlocksPath).createDirectories()

            return repo
        }

        fun connect(repoPath: String): RepoData =
            RepoData(Database.connect("jdbc:sqlite:$repoPath/repo.db", "org.sqlite.JDBC"), Path(repoPath))
    }
}

class InvalidRepoData(val message: String)

fun RepoData.listOfIssues(): List<InvalidRepoData> {
    val issues = mutableListOf<InvalidRepoData>()
    fun report(issue: InvalidRepoData) = issues.add(issue)
    transaction(this.db) {

        // validate file chunk refs are internally consistent
        FileChunkRefs.selectAll().forEach {
            if (it[FileChunkRefs.fromByte] < 0) {
                report(InvalidRepoData("FileChunkRefs fromByte=${it[FileChunkRefs.fromByte]} is negative"))
            }

            if (it[FileChunkRefs.fromByte] >= it[FileChunkRefs.toByte]) {
                report(InvalidRepoData("FileChunkRefs fromByte ${it[FileChunkRefs.fromByte]} >= toByte=${it[FileChunkRefs.toByte]}"))
            }
        }

        // validate file chunk refs indexes are in 0...size of file
        (FileChunkRefs innerJoin FileRefs).selectAll().forEach {
            if (it[FileChunkRefs.toByte] > it[FileRefs.size]) {
                report(InvalidRepoData("FileChunkRefs toByte=${it[FileChunkRefs.toByte]} > size=${it[FileRefs.size]}"))
            }
        }

        // validate file is completely by chunks
        FileRefs.selectAll().forEach { fileRef ->
            val fileHash = fileRef[FileRefs.fileHash]
            val chunksCoverage = FileChunkRefs.selectAll()
                .where { FileChunkRefs.fileHash.eq(fileHash) }
                .map { it[FileChunkRefs.fromByte] to it[FileChunkRefs.toByte] }
                .sortedBy { it.first }

            // check if two chunks overlap
            (0 until chunksCoverage.size - 1).forEach { i ->
                if (chunksCoverage[i].second > chunksCoverage[i + 1].first) {
                    report(InvalidRepoData("Duplicated chunk coverage: ${chunksCoverage[i]} to ${chunksCoverage[i + 1]}"))
                }
            }

            var currentByte: Long = 0
            for (chunk in chunksCoverage) {
                if (currentByte < chunk.first) {
                    report(InvalidRepoData("Gap between $currentByte and ${chunk.first}."))
                }
                currentByte = chunk.second
            }
            if (currentByte < fileRef[FileRefs.size]) {
                report(InvalidRepoData("Gap between $currentByte and ${fileRef[FileRefs.size]}"))
            }
        }

        // validate parity file refs have same sizes as file chunk refs
        (ParityFileRefs innerJoin FileChunkRefs).selectAll().forEach { rows ->
            val parityFileRefSize = rows[ParityFileRefs.toByte] - rows[ParityFileRefs.fromByte]
            val fileChunkSize = rows[FileChunkRefs.toByte] - rows[FileChunkRefs.fromByte]
            if (parityFileRefSize != fileChunkSize) {
                report(InvalidRepoData("ParityFileRef size $parityFileRefSize != FileChunkRef size $fileChunkSize"))
            }
        }

        // validate each parity block references some file
        (ParityBlocks leftJoin ParityFileRefs)
            .select(ParityBlocks.parityHash, ParityFileRefs.fileChunk.count())
            .groupBy(ParityBlocks.parityHash)
            .forEach {
                if (it[ParityFileRefs.fileChunk.count()] == 0L) {
                    report(InvalidRepoData("ParityBlocks ${it[ParityBlocks.parityHash]} is unused!"))
                }
            }
    }
    return issues
}

class RepoTest {
    @Test
    fun `put dummy data in repo db`() {
        val repoPath = "./.experiments/test_insert/.repo"
        RepoData.delete(repoPath)
        val repo: RepoData = RepoData.init(repoPath)

        transaction(repo.db) {
            insertDemoData()
        }

        transaction(repo.db) {
            assertEquals(listOf("whatevs"), ParityBlocks.selectAll().map { it[ParityBlocks.parityHash] })
            assertEquals(124123,
                FileRefs.selectAll()
                    .where { FileRefs.fileHash.eq("dummy_file_hash") }
                    .map { it[FileRefs.size] }
                    .single()
            )
            assertEquals(4234, FileChunkRefs.selectAll().map { it[FileChunkRefs.toByte] }.single())
            assertEquals(1, ParityFileRefs.selectAll().count())
        }
    }

    private fun insertDemoData() {
        ParityBlocks.insert {
            it[parityHash] = "whatevs"
            it[size] = 4096
        }

        FileRefs.insert {
            it[fileHash] = "dummy_file_hash"
            it[size] = 124123
        }

        FileChunkRefs.insert {
            it[fileHash] = "dummy_file_hash"
            it[fromByte] = 256
            it[toByte] = 4234
        }

        ParityFileRefs.insert {
            it[parityBlock] = "whatevs"
            it[fileChunk] = "dummy_file_hash"
            it[fromByte] = 312
            it[toByte] = 456
        }

//        ParityFileRefs.selectAll().single()[ParityBlocks.parityHash] = s
    }

    @Test
    fun `test validation of repo data`() {
        val repoPath = "./.experiments/test_validation/.repo"
        RepoData.delete(repoPath)
        val repo: RepoData = RepoData.init(repoPath)
        transaction(repo.db) {
            insertDemoData()

            ParityBlocks.insert {
                it[parityHash] = "unused"
                it[size] = 8096
            }

        }

        assertFails("[SQLITE_CONSTRAINT_CHECK] A CHECK constraint failed (CHECK constraint failed: fromByte_must_be_positive)") {
            transaction(repo.db) {
                FileChunkRefs.insert {
                    it[fileHash] = "missing_file_hash"
                    it[fromByte] = -1
                    it[toByte] = -10
                }
            }
        }

        for (issue in repo.listOfIssues()) {
            println(issue.message)
        }
    }
}