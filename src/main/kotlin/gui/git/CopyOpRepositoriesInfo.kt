package gui.git

import gui.RemoteInfoQueryResult
import gui.RepositoriesInfoQueryResult

data class CopyOpRepositoriesInfo(
    val loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    val remotesInfo: Map<String, RemoteInfoQueryResult?>
)