package data.repo.sql

import org.jetbrains.exposed.sql.Table

object StorageMedia : Table("storage_media") {
    val guid = text("guid").uniqueIndex()
    val label = text("label")
    val totalSize = integer("size").check { it.greaterEq(0) }
    val freeSize = integer("free_size").check { it.greaterEq(0) }.check { it.lessEq(totalSize) }

    override val primaryKey: PrimaryKey = PrimaryKey(guid)
}