package data.repo.sql

import org.jetbrains.exposed.sql.Table

object FileDataBlockMappings : Table("parity_file_refs") {
    val fileHash = reference("file_hash", FileRefs.fileHash)
    val fromFileIdx = long("from_file_idx").check("fromFileIdx_must_be_nonnegative") { it.greaterEq(0) }
    val chunkSize = long("chunk_size").check("chunk_size_must_be_positive") { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(fileHash, fromFileIdx, chunkSize)

    val parityBlock = reference("parity_block_hash", DataBlocks.hash)
    val fromParityIdx = long("from_parity_idx").check("fromParityIdx_must_be_nonnegative") { it.greaterEq(0) }
}