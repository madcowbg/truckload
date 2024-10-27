package data

import data.parity.BlockMapping
import data.parity.naiveBlockMapping
import data.parity.naiveParitySets
import data.repo.Repo
import data.repo.readFolder
import data.repo.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertIgnore
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.test.Test

@OptIn(ExperimentalEncodingApi::class)
class BuildRepoTest {

    @Test
    fun `read folder on hd and create repo`() {
        val repo: Repo = readFolder(File("./.experiments/data"))
        val blockMapping: BlockMapping = naiveBlockMapping(repo)
        val paritySets = naiveParitySets(blockMapping)

        val repoPath = "./.experiments/test_build/.repo"
        StoredRepo.delete(repoPath)
        val storedRepo: StoredRepo = StoredRepo.init(repoPath)

        for (file in repo.storage) {
            transaction(storedRepo.db) {
                FileRefs.insertIgnore { // ignore because two files can be in different places
                    it[fileHash] = Base64.encode(file.hash)
                    it[size] = file.size
                }
            }
        }

        for (liveBlock in blockMapping.fileBlocks) {
            transaction(storedRepo.db) {
                if (!ParityBlocks.selectAll().where(ParityBlocks.hash.eq(Base64.encode(liveBlock.hash))).empty()) {
                    println(
                        "duplicate parity block ${Base64.encode(liveBlock.hash)} " +
                                "when # of stored is ${ParityBlocks.selectAll().count()}."
                    )
                }

                ParityBlocks.insertIgnore { // blocks can duplicate, e.g. empty or repeating values
                    it[hash] = Base64.encode(liveBlock.hash)
                    it[size] = liveBlock.size
                }

                for (file in liveBlock.files) {
                    ParityFileRefs.insertIgnore { // files with the same content will duplicate
                        it[parityBlock] = Base64.encode(liveBlock.hash)
                        it[fromParityIdx] = 0
                        it[fromFileIdx] = file.from.toLong()
                        it[chunkSize] = file.size.toLong()
                        it[fileHash] = Base64.encode(file.fileHash)
                    }
                }

            }
        }

        storedRepo.listOfIssues().forEach { println(it.message) }
    }
}