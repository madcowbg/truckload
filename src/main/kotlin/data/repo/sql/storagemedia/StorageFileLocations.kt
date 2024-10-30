package data.repo.sql.storagemedia

import data.repo.sql.datablocks.FileRefs
import org.jetbrains.exposed.sql.Table

object StorageFileLocations: Table("storage_file_locations") {
    val storageMedia = reference("storage_media", StorageMedias.guid)
    val path = text("path")

    override val primaryKey: PrimaryKey = PrimaryKey(storageMedia, path)

    val hash = reference("hash", FileRefs.hash)
}