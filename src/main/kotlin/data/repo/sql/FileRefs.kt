package data.repo.sql

import org.jetbrains.exposed.sql.Table

object FileRefs : Table("file_refs") {
    val hash = text("hash").uniqueIndex()
    val size = long("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(hash)
}