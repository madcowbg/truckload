package data.repo.sql

import org.jetbrains.exposed.sql.count
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction

fun StoredRepo.listOfIssues(): List<InvalidRepoData> {
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
            .select(ParityBlocks.hash, ParityFileRefs.fileHash.count())
            .groupBy(ParityBlocks.hash)
            .forEach {
                if (it[ParityFileRefs.fileHash.count()] == 0L) {
                    report(InvalidRepoData("ParityBlocks ${it[ParityBlocks.hash]} is unused!"))
                }
            }

        // validate each file ref is used for at least one file in the catalogue
        (FileRefs leftJoin CatalogueFile)
            .select(FileRefs.fileHash, CatalogueFile.path.count())
            .groupBy(FileRefs.fileHash).forEach {
            if (it[CatalogueFile.path.count()] == 0L) {
                report(InvalidRepoData("FileRefs ${it[FileRefs.fileHash]} is unused!"))
            }
        }
    }
    return issues
}