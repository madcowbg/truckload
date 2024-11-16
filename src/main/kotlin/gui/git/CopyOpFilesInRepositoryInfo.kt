package gui.git

import gui.FileInfoQueryResult
import gui.WhereisQueryResult

data class CopyOpFilesInRepositoryInfo(
    val fileWhereis: Map<String, WhereisQueryResult>,
    val fileInfos: Map<String, FileInfoQueryResult>
)