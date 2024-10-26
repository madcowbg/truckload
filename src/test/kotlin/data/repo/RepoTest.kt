package data.repo

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.io.path.*
import kotlin.test.Test


object ParityBlocks : Table("parity_blocks") {
    val parity_hash = text("parity_hash").uniqueIndex()
    val size = integer("size")

    override val primaryKey = PrimaryKey(parity_hash)
}

object FileChunkRefs : Table("file_chunk_ref") {
    val fileHash = text("file_hash")
    val fromByte = long("from_byte")
    val toByte = long("to_byte")
}

object ParityFileRefs : Table("parity_file_refs") {
    val parityBlock = reference("parity_block_hash", ParityBlocks.parity_hash)
    val fileChunk = reference("file_chunk_hash", FileChunkRefs.fileHash)
    val fromByte = long("from_byte")
    val toByte = long("to_byte")
}


class RepoData private constructor(val repoDb: Database, val repoPath: String) {

    companion object {
        @OptIn(ExperimentalPathApi::class)
        fun delete(repoPath: String) = Path(repoPath).deleteRecursively()

        fun init(repoPath: String): RepoData {
            Path(repoPath).createDirectories()

            val repo = connect(repoPath)
            transaction(repo.repoDb) {
                SchemaUtils.create(ParityBlocks, FileChunkRefs, ParityFileRefs)
            }

            val parityBlocksPath = "$repoPath/parity_blocks"
            Path(parityBlocksPath).createDirectories()

            return repo
        }

        fun connect(repoPath: String): RepoData =
            RepoData(Database.connect("jdbc:sqlite:$repoPath/repo.db", "org.sqlite.JDBC"), repoPath)
    }
}


class RepoTest {
    private val repoPath = "./.experiments/.repo"
    private val repo: RepoData

    init {
        // RepoData.delete(repoPath)
        // RepoData.init(repoPath)
        repo = RepoData.connect(repoPath)
    }

    @Test
    fun `put dummy data in repo db`() {

    }
}