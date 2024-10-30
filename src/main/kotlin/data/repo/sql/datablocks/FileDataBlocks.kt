package data.repo.sql.datablocks

import org.jetbrains.exposed.sql.Table

object FileDataBlocks : Table("file_data_blocks") {
    val hash = text("hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(hash)
}