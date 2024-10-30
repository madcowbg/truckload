package data.repo.sql.parity

import data.repo.sql.datablocks.FileDataBlocks
import org.jetbrains.exposed.sql.Table

object ParityDataBlockMappings: Table("parity_data_block_mappings") {
    val paritySetId = reference("parity_set", ParitySets.hash)
    val indexInSet = integer("index_in_set").check {it.greaterEq(0)}
    val dataBlockHash = reference("data_block_hash", FileDataBlocks.hash)

    override val primaryKey = PrimaryKey(paritySetId, indexInSet)
}