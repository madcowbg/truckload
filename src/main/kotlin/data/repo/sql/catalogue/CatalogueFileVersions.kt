package data.repo.sql.catalogue

import data.repo.sql.datablocks.FileRefs
import org.jetbrains.exposed.sql.Table

object CatalogueFileVersions : Table("catalogue_file_versions"){
    val path = text("path").uniqueIndex()
    val hash = reference("file_hash", FileRefs.hash)
    val state = enumeration("version_state", VersionState::class)

    override val primaryKey = PrimaryKey(path)
}