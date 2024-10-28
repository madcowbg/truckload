package data.repo

import perftest.TestDataSettings
import data.repo.sql.*
import data.repo.sql.datablocks.DataBlocks
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.parity.ParityBlocks
import data.repo.sql.parity.ParityDataBlockMappings
import data.repo.sql.parity.ParitySets
import data.repo.sql.parity.ParityType
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails


class RepoTest {
    @Test
    fun `put dummy data in repo db`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_insert/.repo"
        StoredRepo.delete(repoPath)
        val repo: StoredRepo = StoredRepo.init(repoPath)

        transaction(repo.db) {
            insertDemoData()
        }

        transaction(repo.db) {
            assertEquals(listOf("whatevs"), DataBlocks.selectAll().map { it[DataBlocks.hash] })
            assertEquals(124123,
                FileRefs.selectAll()
                    .where { FileRefs.hash.eq("dummy_file_hash") }
                    .map { it[FileRefs.size] }
                    .single()
            )
            assertEquals(4234, FileDataBlockMappings.selectAll().map { it[FileDataBlockMappings.chunkSize] }.single())
            assertEquals(1, FileDataBlockMappings.selectAll().count())
        }
    }

    private fun insertDemoData() {
        DataBlocks.insert {
            it[hash] = "whatevs"
            it[size] = 4096
        }

        FileRefs.insert {
            it[hash] = "dummy_file_hash"
            it[size] = 124123
        }

        FileDataBlockMappings.insert {
            it[dataBlockHash] = "whatevs"
            it[blockOffset] = 312
            it[chunkSize] = 4234

            it[fileHash] = "dummy_file_hash"
            it[fileOffset] = 256
        }
    }

    @Test
    fun `test validation of repo data`() {
        val repoPath = "${TestDataSettings.test_path}/.experiments/test_validation/.repo"
        StoredRepo.delete(repoPath)
        val repo: StoredRepo = StoredRepo.init(repoPath)
        transaction(repo.db) {
            insertDemoData()

            DataBlocks.insert {
                it[hash] = "unused"
                it[size] = 8096
            }

            ParityBlocks.insert {
                it[hash] = "some_parity"
                it[size] = 4093
            }

            ParityBlocks.insert {
                it[hash] = "some_other_parity"
                it[size] = 4093
            }


            val setId = ParitySets.insertAndGetId {
                it[parityType] = ParityType.RAID6
                it[numDeviceBlocks] = 2
                it[parityPHash] = "some_parity"
                it[parityQHash] = "some_other_parity"
            }

            ParityDataBlockMappings.insert {
                it[dataBlockHash] = "whatevs"
                it[indexInSet] = 2
                it[paritySetId] = setId
            }
        }

        assertFails("[SQLITE_CONSTRAINT_CHECK] A CHECK constraint failed (CHECK constraint failed: fromByte_must_be_positive)") {
            transaction(repo.db) {
                FileDataBlockMappings.insert {
                    it[dataBlockHash] = "whatevs2"
                    it[blockOffset] = -1
                    it[chunkSize] = -1

                    it[fileHash] = "dummy_file_hash"
                    it[fileOffset] = -1
                }
            }
        }

        for (issue in repo.listOfIssues()) {
            println(issue.message)
        }

        assertContentEquals(
            listOf(
                "FileDataBlockMappings ParityBlocks 256 + 4234 > size=4096",
                "Gap between 0 and 256.",
                "Gap between 4490 and 124123",
                "DataBlocks unused is unused!",
                "FileRefs dummy_file_hash is unused!",
                "Data block index=0 is not available in ParityDataBlockMappings[1]",
                "Data block index=1 is not available in ParityDataBlockMappings[1]",
                "ParityDataBlockMappings[1] block index=2 is outside accepted indexes 0..2"
            ),
            repo.listOfIssues().map { it.message }
        )
    }
}