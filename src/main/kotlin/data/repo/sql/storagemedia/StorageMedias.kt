package data.repo.sql.storagemedia

import data.repo.sql.StorageDeviceGUID
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object StorageMedias : Table("storage_media") {
    fun guid(it: ResultRow): StorageDeviceGUID = StorageDeviceGUID(it[guid])

    val guid = text("guid").uniqueIndex()
    val label = text("label")
    val totalSize = integer("total_size").check { it.greaterEq(0) }
    val freeSize = integer("free_size").check { it.greaterEq(0) }.check { it.lessEq(totalSize) }

    override val primaryKey: PrimaryKey = PrimaryKey(guid)
}