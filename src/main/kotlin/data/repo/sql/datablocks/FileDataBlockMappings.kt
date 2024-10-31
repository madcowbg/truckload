package data.repo.sql.datablocks

import data.storage.Hash
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object FileDataBlockMappings : Table("file_data_block_mappings") {

    val fileHash = reference("file_hash", FileRefs.hash)
    val fileOffset = long("file_offset").check("block_offset_must_be_nonnegative") { it.greaterEq(0) }
    val chunkSize = integer("chunk_size").check("chunk_size_must_be_positive") { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(fileHash, fileOffset, chunkSize)

    val dataBlockHash = reference("data_block_hash", FileDataBlocks.hash)
    val blockOffset = integer("block_offset").check("block_offset_must_be_nonnegative") { it.greaterEq(0) }

    fun fileHash(it: ResultRow): Hash = Hash(it[fileHash])
    fun dataBlockHash(it: ResultRow): Hash = Hash(it[dataBlockHash])
}