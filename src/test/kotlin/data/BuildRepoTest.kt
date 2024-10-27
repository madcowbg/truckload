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
                    it[fileHash] = file.hash.storeable
                    it[size] = file.size
                }
            }
        }

        for (liveBlock in blockMapping.fileBlocks) {
            transaction(storedRepo.db) {
                if (!ParityBlocks.selectAll().where(ParityBlocks.hash.eq(liveBlock.hash.storeable)).empty()) {
                    println(
                        "duplicate parity block ${liveBlock.hash} " +
                                "when # of stored is ${ParityBlocks.selectAll().count()}."
                    )
                }

                ParityBlocks.insertIgnore { // blocks can duplicate, e.g. empty or repeating values
                    it[hash] = liveBlock.hash.storeable
                    it[size] = liveBlock.size
                }

                for (file in liveBlock.files) {
                    ParityFileRefs.insertIgnore { // files with the same content will duplicate
                        it[parityBlock] = liveBlock.hash.storeable
                        it[fromParityIdx] = 0
                        it[fromFileIdx] = file.from.toLong()
                        it[chunkSize] = file.size.toLong()
                        it[fileHash] = file.fileHash.storeable
                    }
                }

            }
        }

        storedRepo.listOfIssues().forEach { println(it.message) }
    }
}