package data.repo.sql.datablocks

import org.jetbrains.exposed.sql.Table

object FileDataBlockMappings : Table("file_data_block_mappings") {
    val fileHash = reference("file_hash", FileRefs.hash)
    val fileOffset = long("file_offset").check("block_offset_must_be_nonnegative") { it.greaterEq(0) }
    val chunkSize = long("chunk_size").check("chunk_size_must_be_positive") { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(fileHash, fileOffset, chunkSize)

    val dataBlockHash = reference("data_block_hash", DataBlocks.hash)
    val blockOffset = long("block_offset").check("block_offset_must_be_nonnegative") { it.greaterEq(0) }
}