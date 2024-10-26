package data.repo

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.Test
import kotlin.test.assertContentEquals
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

object ParityFileRefs : Table("parity_file_refs") {
    val parityBlock = reference("parity_block_hash", ParityBlocks.parityHash)

    val chunkSize = long("chunk_size").check("chunk_size_must_be_positive") { it.greaterEq(0) }
    val fromParityIdx = long("from_parity_idx").check("fromParityIdx_must_be_nonnegative") { it.greaterEq(0) }

    val fileHash = reference("file_hash", FileRefs.fileHash)
    val fromFileIdx = long("from_file_idx").check("fromFileIdx_must_be_nonnegative") { it.greaterEq(0) }
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
                SchemaUtils.create(ParityBlocks, FileRefs, ParityFileRefs)
            }

            val parityBlocksPath = "$repoPath/parity_blocks"
            Path(parityBlocksPath).createDirectories()

            return repo
        }

        fun connect(repoPath: String): RepoData = RepoData(
            Database.connect("jdbc:sqlite:$repoPath/repo.db?foreign_keys=on;", "org.sqlite.JDBC"),
            Path(repoPath)
        )
    }
}

class InvalidRepoData(val message: String)

fun RepoData.listOfIssues(): List<InvalidRepoData> {
    val issues = mutableListOf<InvalidRepoData>()
    fun report(issue: InvalidRepoData) = issues.add(issue)
    transaction(this.db) {

        // validate file chunk refs indexes are in 0...size of file
        (ParityFileRefs innerJoin FileRefs).selectAll().forEach {
            if (it[ParityFileRefs.fromFileIdx] + it[ParityFileRefs.chunkSize] > it[FileRefs.size]) {
                report(
                    InvalidRepoData(
                        "ParityFileRefs FileRefs ${it[ParityFileRefs.fromFileIdx]} + ${it[ParityFileRefs.chunkSize]} " +
                                "> size=${it[FileRefs.size]}"
                    )
                )
            }
        }

        (ParityFileRefs innerJoin ParityBlocks).selectAll().forEach {
            if (it[ParityFileRefs.fromParityIdx] + it[ParityFileRefs.chunkSize] > it[ParityBlocks.size]) {
                report(
                    InvalidRepoData(
                        "ParityFileRefs ParityBlocks ${it[ParityFileRefs.fromFileIdx]} + ${it[ParityFileRefs.chunkSize]} " +
                                "> size=${it[ParityBlocks.size]}"
                    )
                )
            }
        }

        // validate file is completely by chunks
        FileRefs.selectAll().forEach { fileRef ->
            val fileHash = fileRef[FileRefs.fileHash]
            val chunksCoverage = ParityFileRefs.selectAll()
                .where { ParityFileRefs.fileHash.eq(fileHash) }
                .map { it[ParityFileRefs.fromFileIdx] to (it[ParityFileRefs.fromFileIdx] + it[ParityFileRefs.chunkSize]) }
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

        // validate each parity block references some file
        (ParityBlocks leftJoin ParityFileRefs)
            .select(ParityBlocks.parityHash, ParityFileRefs.fileHash.count())
            .groupBy(ParityBlocks.parityHash)
            .forEach {
                if (it[ParityFileRefs.fileHash.count()] == 0L) {
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
            assertEquals(4234, ParityFileRefs.selectAll().map { it[ParityFileRefs.chunkSize] }.single())
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

        ParityFileRefs.insert {
            it[parityBlock] = "whatevs"
            it[fromParityIdx] = 312
            it[chunkSize] = 4234

            it[fileHash] = "dummy_file_hash"
            it[fromFileIdx] = 256
        }
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
                ParityFileRefs.insert {
                    it[parityBlock] = "whatevs2"
                    it[fromParityIdx] = -1
                    it[chunkSize] = -1

                    it[fileHash] = "dummy_file_hash"
                    it[fromFileIdx] = -1
                }
            }
        }

        for (issue in repo.listOfIssues()) {
            println(issue.message)
        }

        assertContentEquals(
            listOf(
                "ParityFileRefs ParityBlocks 256 + 4234 > size=4096",
                "Gap between 0 and 256.",
                "Gap between 4490 and 124123",
                "ParityBlocks unused is unused!"
            ),
            repo.listOfIssues().map { it.message }
        )
    }
}