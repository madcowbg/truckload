package data.repo.sql.storagemedia

import data.repo.sql.StorageDeviceGUID
import data.repo.sql.parity.ParityBlocks
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object StorageParityLocations: Table("storage_parity_locations") {
    fun storageMedia(it: ResultRow): StorageDeviceGUID = StorageDeviceGUID(it[storageMedia])

    val hash = reference("hash", ParityBlocks.hash)
    val storageMedia = reference("storage_media", StorageMedias.guid)
}