package gui

import com.xenomachina.argparser.ArgParser
import kotlinx.serialization.Serializable
import me.tongfei.progressbar.*
import kotlinx.serialization.json.Json
import java.io.Closeable
import java.io.File
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.io.PrintStream
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import kotlin.system.exitProcess

class BackupperArgs(parser: ArgParser) {
    val repoDir by parser.storing("-d", help = "git-annex repo directory")
    val backupRepoUUIDs by parser.positionalList(
        "UUID",
        help = "Backup repo UUIDs",
        sizeRange = 1..Int.MAX_VALUE
    )
}

private val verboseOut: PrintStream = System.out

private val jsonDecoder = Json { ignoreUnknownKeys = true }
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

    val repositoriesInfo = loadRepositoriesInfo(repoRoot, backupRepoUUIDs)

    val filesInfo = loadFilesInRepositoryInfo(repositoriesInfo, repoRoot)

    val inAnyBackup = analyzeBackupState(backupRepoUUIDs, filesInfo)

    val op = copyFilesRequiringBackup(repoRoot, backupRepoUUIDs, filesInfo, repositoriesInfo, inAnyBackup)

    op.get()
}

private fun copyFilesRequiringBackup(
    repoRoot: File,
    backupRepoUUIDs: List<String>,
    fileInfosFuture: CompletableFuture<CopyOpFilesInRepositoryInfo>,
    remotesInfoFuture: CompletableFuture<CopyOpRepositoriesInfo>,
    inAnyBackupFuture: CompletableFuture<CopyOnBackupState>
): CompletableFuture<Unit> = CompletableFuture.allOf(fileInfosFuture, remotesInfoFuture, inAnyBackupFuture).thenApply {
    val fileInfos = fileInfosFuture.join()
    val remotesInfo = remotesInfoFuture.join()
    val inAnyBackup = inAnyBackupFuture.join()

    inAnyBackup.inAnyBackup.filter { !it.value }.entries.sortedBy { it.key }.forEach { (file, _) ->
        val fileSize = fileInfos.fileInfos[file]?.size?.toLong() ?: 0
        verboseOut.println("Copying $file of size ${toGB(fileSize)}GB to a backup...")
        verboseOut.println("Finding where...")
        val backupUUID = backupRepoUUIDs.find { uuid ->
            val candidateBackup = remotesInfo.remotesInfo[uuid] ?: return@find false
            val repoLocation = candidateBackup.`repository location` ?: error("Can't find repo location for $uuid?!")
            val fileStore = Files.getFileStore(File(repoLocation).toPath())

            return@find fileSize + driveBufferSize < fileStore.usableSpace
        }
        if (backupUUID == null) {
            error("No place to backup $file - all remotes are full!")
        }
        verboseOut.println("Determined to store to $backupUUID.")

        verboseOut.println("Copying $file to store")
        val gitCopyOperator = GitBatchCopy.toUUID(repoRoot, backupUUID)
            ?: throw IllegalStateException("Can't find operator for $backupUUID")
        val result = gitCopyOperator.executeCopy(file)

        if (result == null) {
            System.err.println("Copy process failed for $file, log tail:")
            gitCopyOperator.tail(3).forEach { System.err.println(it) }
            error("Copy process failed for $file")
        }
    }
    GitBatchCopy.close() // close if one remains open
}

data class CopyOnBackupState(val inAnyBackup: Map<String, Boolean>)

private fun analyzeBackupState(
    backupRepoUUIDs: List<String>,
    filesInfo: CompletableFuture<CopyOpFilesInRepositoryInfo>
): CompletableFuture<CopyOnBackupState> = filesInfo.thenApplyAsync { filesInfo ->
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
    return@thenApplyAsync CopyOnBackupState(inAnyBackup)
}

data class CopyOpRepositoriesInfo(
    val loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    val remotesInfo: Map<String, RemoteInfoQueryResult?>
)

private fun loadRepositoriesInfo(
    repoRoot: File,
    backupRepoUUIDs: List<String>
): CompletableFuture<CopyOpRepositoriesInfo> = CompletableFuture.supplyAsync {
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

    return@supplyAsync CopyOpRepositoriesInfo(loadedRepositoriesInfo, remotesInfo)
}

data class CopyOpFilesInRepositoryInfo(
    val fileWhereis: Map<String, WhereisQueryResult>,
    val fileInfos: Map<String, FileInfoQueryResult>
)

private fun loadFilesInRepositoryInfo(
    reposInfoFuture: CompletableFuture<CopyOpRepositoriesInfo>,
    repoRoot: File
): CompletableFuture<CopyOpFilesInRepositoryInfo> = reposInfoFuture.thenApplyAsync { reposInfo ->
    verboseOut.println("Reading `whereis` information for ${reposInfo.loadedRepositoriesInfo.`annexed files in working tree`} files from ${repoRoot.path}.")

    val fileWhereis = readWhereisInformationForFiles(repoRoot, reposInfo.loadedRepositoriesInfo)

    val fileInfos: Map<String, FileInfoQueryResult> =
        readFilesInfo(reposInfo.loadedRepositoriesInfo, fileWhereis, repoRoot)

    verboseOut.println("Found ${fileInfos.size} files of which ${fileInfos.values.count { it.size != "?" }} have file size")
    verboseOut.println(
        "Found ${fileInfos.size} files in ${repoRoot.path} of total size ${toGB(fileInfos.values.sumOf { it.size.toLong() })}GB"
    )
    return@thenApplyAsync CopyOpFilesInRepositoryInfo(fileWhereis, fileInfos)
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

class GitBatchCopy private constructor(private val repoRoot: File, val toUUID: String) : Closeable {
    private val builder = ProcessBuilder("git", "annex", "copy", "--json", "--from-anywhere", "--to=$toUUID", "--batch")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .directory(repoRoot)

    init {
        verboseOut.println("Batch copy CMD: ${builder.command()}")
    }

    private val process = builder.start()
    private val commandStream = OutputStreamWriter(process.outputStream)
    private val resultStream = InputStreamReader(process.inputStream).buffered()

    private val log = mutableListOf<String>()
    fun tail(n: Int = Int.MAX_VALUE) = log.slice((log.size - n).coerceAtLeast(0) until log.size)

    fun executeCopy(file: String): CopyCmdResult? {
        synchronized(process) {
            verboseOut.println("Copying $file...")

            log.add("[CMD]$file")
            commandStream.write(file + "\n")
            commandStream.flush()

            verboseOut.println("Waiting for results...")
            val resultStr = resultStream.readLine()
            log.add("[RES]$resultStr")
            verboseOut.println("Copying done!")
            return if (resultStr == "") { // operation did nothing
                null
            } else {
                jsonDecoder.decodeFromString(CopyCmdResult.serializer(), resultStr)
            }
        }
    }

    override fun close() {
        process.destroy()
        process.waitFor()
    }

    companion object : Closeable {
        private var copyOperator: GitBatchCopy? = null
        fun toUUID(repoRoot: File, uuid: String): GitBatchCopy? {
            synchronized(Companion) {
                val oldCopyOperator = copyOperator
                if (oldCopyOperator?.toUUID != uuid || oldCopyOperator.repoRoot != repoRoot) {
                    if (oldCopyOperator != null) {
                        synchronized(oldCopyOperator) {
                            oldCopyOperator.close()
                        }
                    }
                    copyOperator = GitBatchCopy(repoRoot, uuid)
                }
                return copyOperator
            }
        }

        override fun close() {
            synchronized(Companion) {
                copyOperator?.close()
                copyOperator = null
            }
        }
    }
}

private fun toGB(bytes: Long) =
    bytes.toFloat() / (1 shl 30)

private fun readFilesInfo(
    loadedRepositoriesInfo: RepositoriesInfoQueryResult,
    fileWhereis: Map<String, WhereisQueryResult>,
    repoRoot: File
): Map<String, FileInfoQueryResult> {
    val pb = ProgressBar("`info`   ", loadedRepositoriesInfo.`annexed files in working tree`!!)
    val fileInfos: Map<String, FileInfoQueryResult> = fileWhereis.keys.chunked(100).flatMap { chunk ->
        pb.stepBy(chunk.size.toLong())
        val process = ProcessBuilder("git", "annex", "info", "--json", "--bytes", *chunk.toTypedArray())
            .directory(repoRoot)
            .start()

        val result = mutableListOf<Pair<String, FileInfoQueryResult>>()
        process.inputStream.bufferedReader().forEachLine { line ->
            val decoded = jsonDecoder.decodeFromString(FileInfoQueryResult.serializer(), line)
            result.add(decoded.file to decoded)
        }
        process.waitFor()
        result
    }.toMap()
    pb.close()
    return fileInfos
}

private fun readWhereisInformationForFiles(
    repoRoot: File,
    loadedRepositoriesInfo: RepositoriesInfoQueryResult
): Map<String, WhereisQueryResult> {
    val process = ProcessBuilder("git", "annex", "whereis", "--json")
        .directory(repoRoot)
        .start()

    val fileWhereis = mutableMapOf<String, WhereisQueryResult>()
    val pb = ProgressBar("`whereis`", loadedRepositoriesInfo.`annexed files in working tree`!!)
    process.inputStream.bufferedReader().forEachLine { line ->
        pb.step()

        val decoded = jsonDecoder.decodeFromString(WhereisQueryResult.serializer(), line)
        if (decoded.file in fileWhereis) {
            System.err.println("File $decoded already exists!")
            exitProcess(-1)
        }
        fileWhereis[decoded.file] = decoded
    }
    process.waitFor()
    pb.close()
    return fileWhereis
}

