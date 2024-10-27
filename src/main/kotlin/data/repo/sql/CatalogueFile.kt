package data.repo.sql

import org.jetbrains.exposed.sql.Table

object CatalogueFile: Table("catalogue_file") {
    val path = text("path").uniqueIndex()
    val hash = reference("hash", FileRefs.fileHash)
}