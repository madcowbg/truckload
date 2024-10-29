package data.repo.sql.storagemedia

import data.repo.sql.parity.ParityBlocks
import org.jetbrains.exposed.sql.Table

object ParityLocations: Table("storage_media_parity_locations") {
    val hash = reference("hash", ParityBlocks.hash)
    val storageMedia = reference("storage_media", StorageMedias.guid)
}