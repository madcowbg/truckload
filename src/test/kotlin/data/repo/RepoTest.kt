package data.repo

import data.repo.sql.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails


class RepoTest {
    @Test
    fun `put dummy data in repo db`() {
        val repoPath = "./.experiments/test_insert/.repo"
        StoredRepo.delete(repoPath)
        val repo: StoredRepo = StoredRepo.init(repoPath)

        transaction(repo.db) {
            insertDemoData()
        }

        transaction(repo.db) {
            assertEquals(listOf("whatevs"), ParityBlocks.selectAll().map { it[ParityBlocks.hash] })
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
            it[hash] = "whatevs"
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
        StoredRepo.delete(repoPath)
        val repo: StoredRepo = StoredRepo.init(repoPath)
        transaction(repo.db) {
            insertDemoData()

            ParityBlocks.insert {
                it[hash] = "unused"
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
                "ParityBlocks unused is unused!",
                "FileRefs dummy_file_hash is unused!"
            ),
            repo.listOfIssues().map { it.message }
        )
    }
}