package data.repo.sql

import org.jetbrains.exposed.sql.Table

object FileVersions : Table("file_versions"){
    val path = text("path").uniqueIndex()
    val hash = reference("file_hash", FileRefs.hash)
    val state = enumeration("version_state", VersionState::class)

    override val primaryKey = PrimaryKey(path)
}