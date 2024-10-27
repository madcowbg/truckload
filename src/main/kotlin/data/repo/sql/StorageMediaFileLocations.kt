package data.repo.sql

import org.jetbrains.exposed.sql.Table

object StorageMediaFileLocations: Table("storage_media_file_locations") {
    val storageMedia = reference("storage_media", StorageMedia.guid)
    val path = text("path")

    val hash = reference("hash", FileRefs.hash)

    override val primaryKey: PrimaryKey = PrimaryKey(storageMedia, path)
}