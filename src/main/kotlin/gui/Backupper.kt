package gui

import com.xenomachina.argparser.ArgParser
import me.tongfei.progressbar.*
import kotlinx.serialization.json.Json
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

private val verboseOut: PrintStream = System.out

private val jsonDecoder = Json { ignoreUnknownKeys = true }
private const val driveBufferSize = 1 shl 30 // 1GB

fun main(args: Array<String>) = ArgParser(args).parseInto(::BackupperArgs).run {
    val repoRoot = File(repoDir)
    if (!repoRoot.exists()) {
        System.err.println("Repo $repoDir does not exist!")
        exitProcess(-1)
    }
    if (!repoRoot.isDirectory) {
        System.err.println("Repo $repoDir is not a directory!")
        exitProcess(-1)
    }

    verboseOut.println("Loading git-annex info in $repoDir")
    val loadedRepositoriesInfo =
        Git.executeOnAnnex(repoRoot, RepositoriesInfoQueryResult.serializer(), "info", "--json").get()
    if (loadedRepositoriesInfo == null) {
        System.err.println("Could not load info!")
        exitProcess(-1)
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
            exitProcess(-1)
        }
    }

    verboseOut.println("Loading backup remotes data...")
    val remotesInfo: Map<String, RemoteInfoQueryResult?> = backupRepoUUIDs.associateWith { uuid ->
        Git.executeOnAnnex(repoRoot, RemoteInfoQueryResult.serializer(), "info", "--json", uuid).get()
    }

    verboseOut.println("Reading `whereis` information for ${loadedRepositoriesInfo.`annexed files in working tree`} files from $repoDir.")

    val fileWhereis = readWhereisInformationForFiles(repoRoot, loadedRepositoriesInfo)

    val fileInfos: Map<String, FileInfoQueryResult> = readFilesInfo(loadedRepositoriesInfo, fileWhereis, repoRoot)

    verboseOut.println("Found ${fileInfos.size} files of which ${fileInfos.values.count { it.size != "?" }} have file size")
    verboseOut.println(
        "Found ${fileInfos.size} files in $repoDir of total size ${toGB(fileInfos.values.sumOf { it.size.toLong() })}GB"
    )

    val storedFilesPerBackupRepo = backupRepoUUIDs
        .associateWith { backupUuid ->
            fileWhereis
                .filterValues { it.whereis.any { loc -> loc.uuid == backupUuid } }
                .values.toList()
        }
    val inAnyBackup: Map<String, Boolean> =
        fileWhereis.mapValues { (file, info) -> info.whereis.any { loc -> loc.uuid in backupRepoUUIDs } }

    verboseOut.println("Repo contents:")
    verboseOut.println("total # files: ${fileWhereis.size}")
    verboseOut.println(
        "# files in a backup: ${inAnyBackup.count { it.value }}, " +
                "${toGB(inAnyBackup.filter { it.value }.keys.sumOf { fileInfos[it]?.size?.toLong() ?: 0 })} GB"
    )
    verboseOut.println(
        "# files in no backup: ${inAnyBackup.count { !it.value }}, " +
                "${toGB(inAnyBackup.filter { !it.value }.keys.sumOf { fileInfos[it]?.size?.toLong() ?: 0 })} GB"
    )

    storedFilesPerBackupRepo.forEach { (backupRepoUuid, files) ->
        verboseOut.println("  $backupRepoUuid: ${files.size} files")
    }

//    return
    inAnyBackup.filter { !it.value }.entries.sortedBy { it.key }.forEach { (file, _) ->
        val fileSize = fileInfos[file]?.size?.toLong() ?: 0
        verboseOut.println("Copying $file of size ${toGB(fileSize)}GB to a backup...")
        verboseOut.println("Finding where...")
        val uuid = backupRepoUUIDs.find { uuid ->
            val candidateBackup = remotesInfo[uuid] ?: return@find false
            val repoLocation = candidateBackup.`repository location` ?: error("Can't find repo location for $uuid?!")
            val fileStore = Files.getFileStore(File(repoLocation).toPath())

            return@find fileSize + driveBufferSize < fileStore.usableSpace
        }
        if (uuid == null) {
            error("No place to backup $file - all remotes are full!")
        }
        verboseOut.println("Determined to store to $uuid.")

        verboseOut.println("Copying $file to store")
        val builder = ProcessBuilder("git", "annex", "copy", "--json", "--from-anywhere", "--to=$uuid", file)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .directory(repoRoot)
        verboseOut.println("Cmd: ${builder.command()}")
        val process = builder.start()

        process.waitFor()
//        println("Process exited: ${process.exitValue()}")
        if (process.exitValue() != 0)  {
            error("Copy process failed with error ${process.errorStream.bufferedReader().readText()}")
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

