package data.repo.sql

import data.repo.sql.datablocks.FileRefs
import org.jetbrains.exposed.sql.Table

object CatalogueFile: Table("catalogue_file") {
    val path = text("path").uniqueIndex()
    val hash = reference("hash", FileRefs.hash)
}