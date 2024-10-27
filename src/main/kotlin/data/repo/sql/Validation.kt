package data.repo.sql

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
                        "ParityFileRefs FileRefs ${it[FileDataBlockMappings.fileOffset]} + ${it[FileDataBlockMappings.chunkSize]} " +
                                "> size=${it[FileRefs.size]}"
                    )
                )
            }
        }

        (FileDataBlockMappings innerJoin DataBlocks).selectAll().forEach {
            if (it[FileDataBlockMappings.blockOffset] + it[FileDataBlockMappings.chunkSize] > it[DataBlocks.size]) {
                report(
                    InvalidRepoData(
                        "ParityFileRefs ParityBlocks ${it[FileDataBlockMappings.fileOffset]} + ${it[FileDataBlockMappings.chunkSize]} " +
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
                    report(InvalidRepoData("ParityBlocks ${it[DataBlocks.hash]} is unused!"))
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
    }
    return issues
}