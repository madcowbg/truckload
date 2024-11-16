package gui

import com.xenomachina.argparser.ArgParser
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import me.tongfei.progressbar.*
import kotlinx.serialization.json.Json
import java.io.Closeable
import java.io.File
import java.io.PrintStream
import java.nio.file.Files
import kotlin.system.exitProcess

class BackupperArgs(parser: ArgParser) {
    val repoDir by parser.storing("-d", help = "git-annex repo directory")
    val backupRepoUUIDs by parser.positionalList(
        "UUID",
        help = "Backup repo UUIDs",
        sizeRange = 1..Int.MAX_VALUE
    )
}

val verboseOut: PrintStream = System.out

val jsonDecoder = Json { ignoreUnknownKeys = true }
private const val driveBufferSize: Long = 50 * (1L shl 30) // 50GB

fun main(args: Array<String>): Unit = ArgParser(args).parseInto(::BackupperArgs).run {
    val repoRoot = File(repoDir)
    if (!repoRoot.exists()) {
        System.err.println("Repo $repoDir does not exist!")
        exitProcess(-1)
    }
    if (!repoRoot.isDirectory) {
        System.err.println("Repo $repoDir is not a directory!")
        exitProcess(-1)
    }
    runBlocking {
        val repositoriesInfo = async { loadRepositoriesInfo(repoRoot, backupRepoUUIDs) }

        val filesInfo = loadFilesInRepositoryInfo(repositoriesInfo, repoRoot)

        val inAnyBackup = analyzeBackupState(backupRepoUUIDs, filesInfo)

        copyFilesRequiringBackup(repoRoot, backupRepoUUIDs, filesInfo, repositoriesInfo, inAnyBackup)
    }
}

suspend fun copyFilesRequiringBackup(
    repoRoot: File,
    backupRepoUUIDs: List<String>,
    fileInfos: CopyOpFilesInRepositoryInfo,
    remotesInfoFuture: Deferred<CopyOpRepositoriesInfo>,
    inAnyBackup: CopyOnBackupState
) {
    val remotesInfo = remotesInfoFuture.await()

    GitBatchCopy(repoRoot).use { copyOp ->
        for (file in inAnyBackup.sortedFiles) {
            val backupUUID = chooseBackupDestination(remotesInfo, fileInfos, backupRepoUUIDs, file)
                ?: error("No place to backup $file - all remotes are full!")

            backupFileToRemote(backupUUID = backupUUID, file = file, copyOp)
        }
    }
}

suspend fun backupFileToRemote(backupUUID: String, file: String, copyOp: GitBatchCopy) {
    verboseOut.println("Determined to store to $backupUUID.")
    verboseOut.println("Copying $file to store")

    copyOp.executeCopy(file, backupUUID) ?: throw IllegalStateException("Copy process failed for $file")
}

fun chooseBackupDestination(
    remotesInfo: CopyOpRepositoriesInfo,
    fileInfos: CopyOpFilesInRepositoryInfo,
    backupRepoUUIDs: List<String>,
    file: String
): String? {
    val fileSize = fileInfos.fileInfos[file]?.size?.toLong() ?: 0
    verboseOut.println("Copying $file of size ${toGB(fileSize)}GB to a backup...")
    verboseOut.println("Finding where...")
    val backupUUID = backupRepoUUIDs.find { uuid ->
        val candidateBackup = remotesInfo.remotesInfo[uuid] ?: return@find false
        val repoLocation = candidateBackup.`repository location` ?: error("Can't find repo location for $uuid?!")
        val fileStore = Files.getFileStore(File(repoLocation).toPath())

        return@find fileSize + driveBufferSize < fileStore.usableSpace
    }
    return backupUUID
}

data class CopyOpRepositoriesInfo(
    val loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    val remotesInfo: Map<String, RemoteInfoQueryResult?>
)

suspend fun loadRepositoriesInfo(
    repoRoot: File,
    backupRepoUUIDs: List<String>
): CopyOpRepositoriesInfo {
    verboseOut.println("Loading git-annex info in ${repoRoot.path}")
    val loadedRepositoriesInfo =
        Git.executeOnAnnex(repoRoot, RepositoriesInfoQueryResult.serializer(), "info", "--json").get()
    if (loadedRepositoriesInfo == null) {
        System.err.println("Could not load info!")
        throw IllegalStateException("Could not load repo info!")
    }

    val allRepos: Map<String, RepositoryDescription> =
        (loadedRepositoriesInfo.`untrusted repositories` +
                loadedRepositoriesInfo.`semitrusted repositories` +
                loadedRepositoriesInfo.`trusted repositories`)
            .associateBy { it.uuid }

    verboseOut.println("Found ${allRepos.size} repositories!")

    backupRepoUUIDs.forEach { uuid ->
        if (uuid !in allRepos) {
            verboseOut.println("$uuid not found in list of existing repos!")
            throw IllegalStateException("$uuid not found in list of existing repos!")
        }
    }

    verboseOut.println("Loading backup remotes data...")
    val remotesInfo: Map<String, RemoteInfoQueryResult?> = backupRepoUUIDs.associateWith { uuid ->
        Git.executeOnAnnex(repoRoot, RemoteInfoQueryResult.serializer(), "info", "--json", uuid).get()
    }

    return CopyOpRepositoriesInfo(loadedRepositoriesInfo, remotesInfo)
}

data class CopyOpFilesInRepositoryInfo(
    val fileWhereis: Map<String, WhereisQueryResult>,
    val fileInfos: Map<String, FileInfoQueryResult>
)

typealias ProgressCallback = (current: Int, max: Int) -> Unit

suspend fun loadFilesInRepositoryInfo(
    reposInfoFuture: Deferred<CopyOpRepositoriesInfo>,
    repoRoot: File,
    whereisProgressCallback: ProgressCallback = { _, _ -> },
    infoProgressCallback: ProgressCallback = { _, _ -> },
): CopyOpFilesInRepositoryInfo {
    val reposInfo = reposInfoFuture.await()
    verboseOut.println("Reading `whereis` information for ${reposInfo.loadedRepositoriesInfo.`annexed files in working tree`} files from ${repoRoot.path}.")

    val fileWhereis =
        readWhereisInformationForFiles(repoRoot, reposInfo.loadedRepositoriesInfo, whereisProgressCallback)

    val fileInfos: Map<String, FileInfoQueryResult> =
        readFilesInfo(reposInfo.loadedRepositoriesInfo, fileWhereis, repoRoot, infoProgressCallback)

    verboseOut.println("Found ${fileInfos.size} files of which ${fileInfos.values.count { it.size != "?" }} have file size")
    verboseOut.println(
        "Found ${fileInfos.size} files in ${repoRoot.path} of total size ${toGB(fileInfos.values.sumOf { it.size.toLong() })}GB"
    )
    return CopyOpFilesInRepositoryInfo(fileWhereis, fileInfos)
}

data class CopyOnBackupState(
    val inAnyBackup: Map<String, Boolean>,
    val storedFilesPerBackupRepo: Map<String, List<WhereisQueryResult>>
) {
    val sortedFiles = inAnyBackup
        .filter { !it.value }
        .keys.sorted()
}

fun analyzeBackupState(
    backupRepoUUIDs: List<String>,
    filesInfo: CopyOpFilesInRepositoryInfo
): CopyOnBackupState {
    val storedFilesPerBackupRepo = backupRepoUUIDs
        .associateWith { backupUuid ->
            filesInfo.fileWhereis
                .filterValues { it.whereis.any { loc -> loc.uuid == backupUuid } }
                .values.toList()
        }
    val inAnyBackup: Map<String, Boolean> =
        filesInfo.fileWhereis.mapValues { (_, info) -> info.whereis.any { loc -> loc.uuid in backupRepoUUIDs } }

    verboseOut.println("Repo contents:")
    verboseOut.println("total # files: ${filesInfo.fileWhereis.size}")
    verboseOut.println(
        "# files in a backup: ${inAnyBackup.count { it.value }}, " +
                "${toGB(inAnyBackup.filter { it.value }.keys.sumOf { filesInfo.fileInfos[it]?.size?.toLong() ?: 0 })} GB"
    )
    verboseOut.println(
        "# files in no backup: ${inAnyBackup.count { !it.value }}, " +
                "${toGB(inAnyBackup.filter { !it.value }.keys.sumOf { filesInfo.fileInfos[it]?.size?.toLong() ?: 0 })} GB"
    )

    storedFilesPerBackupRepo.forEach { (backupRepoUuid, files) ->
        verboseOut.println("  $backupRepoUuid: ${files.size} files")
    }
    return CopyOnBackupState(inAnyBackup, storedFilesPerBackupRepo)
}

//{"command":"copy",
// "error-messages":[],
// "file":"bulkexport\\VID_20230507_125125_001\\default_preview.mp4",
// "input":["bulkexport\\VID_20230507_125125_001\\default_preview.mp4"],
// "key":"SHA256E-s1660709755--d3abb4d455340d73215d1f98b0648c001fcffa96820b8f2f315480d279a8128a.mp4",
// "note":"from backups-vol-01-Insta360...",
// "success":true}
@Serializable
data class CopyCmdResult(
    val file: String,
    //@SerialName("error-messages") val errorMessages: List<Object>,
    val success: Boolean
)

class GitBatchCopy(private val repoRoot: File) : Closeable {
    var uuidOfCommand: String? = null
        set(value) {
            if (value != field) {
                field = value
                cmd?.close()
                cmd = GitBatchCommand(repoRoot, "annex", "copy", "--json", "--from-anywhere", "--to=$value", "--batch")
            } else {
                field = value
            }
        }

    var cmd: GitBatchCommand? = null
        private set

    suspend fun executeCopy(file: String, toUUID: String): CopyCmdResult? {
        this.uuidOfCommand = toUUID
        val cmd = this.cmd ?: return null

        verboseOut.println("Batch copy CMD: ${cmd.builder?.command()}")

        verboseOut.println("Copying $file...")

        val result = cmd.runOnce(batchUnitCmd = file) { resultStr ->
            if (resultStr == "") { // operation did nothing
                null
            } else {
                jsonDecoder.decodeFromString(CopyCmdResult.serializer(), resultStr)
            }
        }

        verboseOut.println("Copying done!")
        return result
    }

    override fun close() {
        cmd?.close()
        cmd = null
    }

    val isRunning: Boolean
        get() = cmd != null
}

fun toGB(bytes: Long) =
    bytes.toFloat() / (1 shl 30)


private fun readFilesInfo(
    loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    fileWhereis: Map<String, WhereisQueryResult>,
    repoRoot: File,
    infoProgressCallback: ProgressCallback
): Map<String, FileInfoQueryResult> {
    var current = 0
    val fileInfos: Map<String, FileInfoQueryResult> =
        ProgressBar("`info`   ", loadedRepositoriesInfo.`annexed files in working tree`!!).use {
            fileWhereis.keys.chunked(100).flatMap { chunk ->
                it.stepBy(chunk.size.toLong())

                val cmd = GitJsonCommand(
                    repoRoot, FileInfoQueryResult.serializer(),
                    "annex", "info", "--json", "--bytes", *chunk.toTypedArray()
                )

                current += chunk.size
                infoProgressCallback(it.current.toInt(), it.max.toInt())

                cmd.results.map { decoded -> decoded.file to decoded }.toList()
            }.toMap()
        }
    return fileInfos
}

private fun readWhereisInformationForFiles(
    repoRoot: File,
    loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    whereisProgressCallback: ProgressCallback
): Map<String, WhereisQueryResult> {
    val fileWhereis = mutableMapOf<String, WhereisQueryResult>()
    ProgressBar("`whereis`", loadedRepositoriesInfo.`annexed files in working tree`!!).use { pb ->
        GitJsonCommand(repoRoot, WhereisQueryResult.serializer(), "annex", "whereis", "--json").use {
            it.results.forEach { decoded ->
                pb.step()

                if (decoded.file in fileWhereis) {
                    System.err.println("File $decoded already exists!")
                    exitProcess(-1)
                }

                whereisProgressCallback(pb.current.toInt(), pb.max.toInt())
                fileWhereis[decoded.file] = decoded
            }
        }
    }

    return fileWhereis
}

