package data.repo.sql

import org.jetbrains.exposed.sql.Table

object ParityBlocks : Table("parity_blocks") {
    val parityHash = text("parity_hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(parityHash)
}