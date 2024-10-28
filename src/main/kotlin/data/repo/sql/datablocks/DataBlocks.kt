package data.repo.sql.datablocks

import org.jetbrains.exposed.sql.Table

object DataBlocks : Table("data_blocks") {
    val hash = text("hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(hash)
}