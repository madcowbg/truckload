package data.repo.sql

import org.jetbrains.exposed.sql.Table

object FileRefs : Table("file_refs") {
    val fileHash = text("file_hash").uniqueIndex()
    val size = integer("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(fileHash)
}