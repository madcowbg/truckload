package data.repo.sql.datablocks

import data.repo.sql.FileSize
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object FileRefs : Table("file_refs") {
    fun size(row: ResultRow): FileSize = FileSize(row[size])

    val hash = text("hash").uniqueIndex()
    val size = long("size").check { it.greaterEq(0) }

    override val primaryKey = PrimaryKey(hash)
}