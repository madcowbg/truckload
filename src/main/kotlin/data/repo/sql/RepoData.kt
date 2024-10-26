package data.repo.sql

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import java.nio.file.Path
import kotlin.io.path.*

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