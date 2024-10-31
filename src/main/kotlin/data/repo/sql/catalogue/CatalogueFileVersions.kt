package data.repo.sql.catalogue

import data.repo.sql.FileSize
import data.repo.sql.datablocks.FileRefs
import data.storage.Hash
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object CatalogueFileVersions : Table("catalogue_file_versions"){
    fun hash(row: ResultRow): Hash = Hash(row[hash])

    val path = text("path").uniqueIndex()
    val hash = reference("file_hash", FileRefs.hash)
    val state = enumeration("version_state", VersionState::class)

    override val primaryKey = PrimaryKey(path)
}