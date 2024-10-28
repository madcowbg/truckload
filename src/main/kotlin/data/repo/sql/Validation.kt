package data.repo.sql

import data.repo.sql.datablocks.DataBlocks
import data.repo.sql.datablocks.FileDataBlockMappings
import data.repo.sql.datablocks.FileRefs
import data.repo.sql.parity.ParityDataBlockMappings
import data.repo.sql.parity.ParitySets
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.count
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction

fun StoredRepo.listOfIssues(): List<InvalidRepoData> {
    val issues = mutableListOf<InvalidRepoData>()
    fun report(issue: InvalidRepoData) = issues.add(issue)
    transaction(this.db) {

        // validate file chunk refs indexes are in 0...size of file
        (FileDataBlockMappings innerJoin FileRefs).selectAll().forEach {
            if (it[FileDataBlockMappings.fileOffset] + it[FileDataBlockMappings.chunkSize] > it[FileRefs.size]) {
                report(
                    InvalidRepoData(
                        "FileDataBlockMappings FileRefs ${it[FileDataBlockMappings.fileOffset]} + ${it[FileDataBlockMappings.chunkSize]} " +
                                "> size=${it[FileRefs.size]}"
                    )
                )
            }
        }

        (FileDataBlockMappings innerJoin DataBlocks).selectAll().forEach {
            if (it[FileDataBlockMappings.blockOffset] + it[FileDataBlockMappings.chunkSize] > it[DataBlocks.size]) {
                report(
                    InvalidRepoData(
                        "FileDataBlockMappings ParityBlocks ${it[FileDataBlockMappings.fileOffset]} + ${it[FileDataBlockMappings.chunkSize]} " +
                                "> size=${it[DataBlocks.size]}"
                    )
                )
            }
        }

        // validate file is completely by chunks
        FileRefs.selectAll().forEach { fileRef ->
            val fileHash = fileRef[FileRefs.hash]
            val chunksCoverage = FileDataBlockMappings.selectAll()
                .where { FileDataBlockMappings.fileHash.eq(fileHash) }
                .map { it[FileDataBlockMappings.fileOffset] to (it[FileDataBlockMappings.fileOffset] + it[FileDataBlockMappings.chunkSize]) }
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
        (DataBlocks leftJoin FileDataBlockMappings)
            .select(DataBlocks.hash, FileDataBlockMappings.fileHash.count())
            .groupBy(DataBlocks.hash)
            .forEach {
                if (it[FileDataBlockMappings.fileHash.count()] == 0L) {
                    report(InvalidRepoData("DataBlocks ${it[DataBlocks.hash]} is unused!"))
                }
            }

        // validate each file ref is used for at least one file in the catalogue
        (FileRefs leftJoin CatalogueFile)
            .select(FileRefs.hash, CatalogueFile.path.count())
            .groupBy(FileRefs.hash).forEach {
                if (it[CatalogueFile.path.count()] == 0L) {
                    report(InvalidRepoData("FileRefs ${it[FileRefs.hash]} is unused!"))
                }
            }

        // validate parity data block mappings are 0..numDeviceBlocksInSet
        ParitySets.selectAll().forEach { paritySet ->
            val id = paritySet[ParitySets.id]
            val numDeviceBlocksInSet = paritySet[ParitySets.numDeviceBlocks]
            val dataBlockIdxs = ParityDataBlockMappings.selectAll().where(ParityDataBlockMappings.paritySetId.eq(id))
                .map { it[ParityDataBlockMappings.indexInSet] }.sorted()

            (0 until numDeviceBlocksInSet).filter { it !in dataBlockIdxs }.forEach {
                report(InvalidRepoData("Data block index=$it is not available in ParityDataBlockMappings[$id]"))
            }
            dataBlockIdxs.filter { it !in 0 until numDeviceBlocksInSet }.forEach {
                report(InvalidRepoData("ParityDataBlockMappings[$id] block index=$it is outside accepted indexes 0..${numDeviceBlocksInSet}"))
            }
        }
    }
    return issues
}