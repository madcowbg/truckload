package gui.git

import gui.WhereisQueryResult

data class CopyOnBackupState(
    val inAnyBackup: Map<String, Boolean>,
    val storedFilesPerBackupRepo: Map<String, List<WhereisQueryResult>>
) {
    val sortedFiles = inAnyBackup
        .filter { !it.value }
        .keys.sorted()
}