package data.repo.sql

import org.jetbrains.exposed.sql.Table

object ParityLocations: Table("parity_locations") {
    val hash = reference("hash", ParityBlocks.hash)
    val storageMedia = reference("storage_media", StorageMedia.guid)
}