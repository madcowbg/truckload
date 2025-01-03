package gui

import glm_.vec4.Vec4
import gui.UISelection.selectedFile
import gui.UISelection.selectedRepo
import gui.deprecated.*
import gui.git.CopyOnBackupState
import gui.git.CopyOpFilesInRepositoryInfo
import gui.git.CopyOpRepositoriesInfo
import imgui.ImGui
import imgui.ImGui.sameLine
import imgui.ImGui.separator
import imgui.ImGui.text
import imgui.ImGui.textColored
import imgui.MutableProperty
import imgui.dsl
import imgui.dsl.tabBar
import imgui.dsl.tabItem
import kotlinx.coroutines.*
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.Closeable
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.ConcurrentHashMap

fun runMainUILoop() {
    showSettingsWindow()

    showSelectedRepoContentsWindow()

    showSelectedFileDetailsWindow()

    showGitExecutionStateWindow()

    showRepoInformationWindow()

    if (UISelection.showBackupperWindow) {
        showBackupperWindow()
    }
}

val RED = Vec4(1f, .2f, .2f, 1f)
val GREEN = Vec4(.2f, 1f, .2f, 1f)
val GRAY = Vec4(.7f, .7f, .7f, 1f)
val YELLOW = Vec4(.6f, .6f, .2f, 1f)

class BackupperUI(val repoRoot: File, backupRepoUUIDs: List<String>) : Closeable,
    CoroutineScope by CoroutineScope(Dispatchers.Default) {
    val backupRepoUUIDs: List<String> = ArrayList(backupRepoUUIDs)
    var repositoriesInfo: Deferred<CopyOpRepositoriesInfo>? = null
    var filesInfo: Deferred<CopyOpFilesInRepositoryInfo>? = null
    var inAnyBackup: Deferred<CopyOnBackupState>? = null
    var scheduledToCopy: Deferred<MutableList<String>>? = null

    var runningOp: Job? = null

    var copyOp = GitBatchCopy(repoRoot)

    override fun close() {
        copyOp.close()

        repositoriesInfo?.cancel("Closing BackupperUI")
        filesInfo?.cancel("Closing BackupperUI")
        inAnyBackup?.cancel("Closing BackupperUI")
        scheduledToCopy?.cancel("Closing BackupperUI")

        this.cancel("Stopping UI!")
    }

    fun triggerLoadRepositoriesInfo() {
        repositoriesInfo?.cancel("Stopping old")
        repositoriesInfo = async { loadRepositoriesInfo(repoRoot, backupRepoUUIDs) }
    }

    class ProgressInfo(var current: Int, var max: Int) {
        val fraction: Float
            get() = current.toFloat() / max
    }

    val whereisProgress = ProgressInfo(0, Int.MAX_VALUE)
    val infoProgress = ProgressInfo(0, Int.MAX_VALUE)

    fun triggerLoadFilesInfo() {
        filesInfo?.cancel("Stopping old")
        filesInfo = repositoriesInfo?.let {
            async {
                loadFilesInRepositoryInfo(it, repoRoot,
                    { currentWhereis, maxWhereis ->
                        whereisProgress.current = currentWhereis
                        whereisProgress.max = maxWhereis
                    }, { currentInfo, maxInfo ->
                        infoProgress.current = currentInfo
                        infoProgress.max = maxInfo
                    })
            }
        }

        inAnyBackup?.cancel("Stopping old")
        inAnyBackup = filesInfo?.let { async { analyzeBackupState(backupRepoUUIDs, it.await()) } }

        scheduledToCopy?.cancel("Stopping old")
        scheduledToCopy = inAnyBackup?.let { async { ArrayList(it.await().sortedFiles) } }
    }

    suspend fun triggerCopy(file: String, onSuccess: () -> Unit) = coroutineScope {
        if (runningOp != null) {
            System.err.println("Cancelling current op as we triggered copy of $file")
        }
        runningOp?.cancel("Cancelling current op as we triggered copy of $file")

        runningOp = launch {
            val fileInfos = filesInfo?.await() ?: return@launch
            val remotesInfo = repositoriesInfo?.await() ?: return@launch

            val backupUUID = chooseBackupDestination(remotesInfo, fileInfos, backupRepoUUIDs, file)
                ?: return@launch

            backupFileToRemote(backupUUID = backupUUID, file = file, copyOp)

            onSuccess()
        }
    }
}


var currentBackupper: BackupperUI? = null
private fun getBackupper(currentRepo: RepoUI, backupRepoUUIDs: List<String>): BackupperUI {
    var backupperUI = currentBackupper
    if (backupperUI?.repoRoot != currentRepo.repo.root || backupRepoUUIDs != backupperUI.backupRepoUUIDs) {
        backupperUI?.close()
        backupperUI = BackupperUI(repoRoot = currentRepo.repo.root, backupRepoUUIDs = backupRepoUUIDs)
        currentBackupper = backupperUI
    }
    return backupperUI
}

val selectedBackupRepoUUIDs: MutableList<String> = mutableListOf()

fun showBackupperWindow() {
    ImGui.begin("Backupper")
    val currentRepo = selectedRepo
    if (currentRepo == null) {
        ImGui.text("Select repo first!")
    } else {
        val backupperUI = getBackupper(currentRepo, selectedBackupRepoUUIDs)
        ImGui.text("Repo: ${backupperUI.repoRoot}")
        ImGui.text("Backups:")
        backupperUI.backupRepoUUIDs.forEach { ImGui.text(it) }
        separator()

        val repositoriesInfo = backupperUI.repositoriesInfo
        if (repositoriesInfo == null) {
            if (ImGui.button("Read Repositories Info")) {
                backupperUI.triggerLoadRepositoriesInfo()
            }
        } else if (!repositoriesInfo.isCompleted) {
            ImGui.textColored(YELLOW, "Loading Repositories Info...")
        } else { // has repo info
            val ri = runBlocking { repositoriesInfo.await() }
            ImGui.text("Found ${ri.remotesInfo.size} repositories!") // TODO more info
            ImGui.text("Need to read `whereis` information for ${ri.loadedRepositoriesInfo.`annexed files in working tree`} files.")
        }

        val filesInfo = backupperUI.filesInfo
        if (repositoriesInfo != null) {
            if (filesInfo == null) {
                if (ImGui.button("Read Files")) {
                    backupperUI.triggerLoadFilesInfo()
                }
            } else if (!filesInfo.isCompleted) {
                text("Loading Whereis Info...")
                sameLine(200)
                ImGui.progressBar(backupperUI.whereisProgress.fraction)

                text("Loading Files Info...")
                sameLine(200)
                ImGui.progressBar(backupperUI.infoProgress.fraction)
            } else { // has files info
                val fi = runBlocking { filesInfo.await() }

                ImGui.text("Found ${fi.fileInfos.size} files of which ${fi.fileInfos.values.count { it.size != "?" }} have file size")
                ImGui.text(
                    "Found ${fi.fileInfos.size} files of total size ${toGB(fi.fileInfos.values.sumOf { it.size.toLong() })}GB"
                )
            }
        }

        val inAnyBackup = backupperUI.inAnyBackup
        if (inAnyBackup == null || filesInfo?.isCompleted != true) {
            // show nothing, previous step triggers it
        } else if (!inAnyBackup.isCompleted) {
            ImGui.textColored(YELLOW, "Creating backup strategy...")
        } else { // has a strategy!
            val fi = filesInfo.getCompleted()
            val ia = inAnyBackup.getCompleted()
            ImGui.text("Repo contents:")
            ImGui.text("total # files: ${fi.fileWhereis.size}")
            ImGui.text(
                "# files in a backup: ${ia.inAnyBackup.count { it.value }}, " +
                        "${toGB(ia.inAnyBackup.filter { it.value }.keys.sumOf { fi.fileInfos[it]?.size?.toLong() ?: 0 })} GB"
            )
            ImGui.text(
                "# files in no backup: ${ia.inAnyBackup.count { !it.value }}, " +
                        "${toGB(ia.inAnyBackup.filter { !it.value }.keys.sumOf { fi.fileInfos[it]?.size?.toLong() ?: 0 })} GB"
            )
            ia.storedFilesPerBackupRepo.forEach { (backupRepoUuid, files) ->
                ImGui.text("  $backupRepoUuid: ${files.size} files")
            }
        }
        separator()

        val status = if (backupperUI.copyOp.isRunning) "(running)" else "(stopped)"
        if (backupperUI.runningOp != null) ImGui.beginDisabled()
        ImGui.checkbox("Enable Batch Copy", UISelection::enableGitBatch); sameLine(); ImGui.text(status)
        if (backupperUI.runningOp != null) ImGui.endDisabled()

        val scheduledToCopy = backupperUI.scheduledToCopy

        if (!UISelection.enableGitBatch) ImGui.beginDisabled()
        ImGui.checkbox("Auto", UISelection::enableAutoCopy)
        sameLine()
        val countToCopy = scheduledToCopy?.ifPresent { it.size } ?: 0
        ImGui.text("($countToCopy)")
        if (!UISelection.enableGitBatch) ImGui.endDisabled()

        if (backupperUI.runningOp?.isCompleted == true) {
            backupperUI.runningOp = null
        }


        if (UISelection.enableAutoCopy && UISelection.enableGitBatch) {
            if (backupperUI.runningOp == null && scheduledToCopy?.isCompleted == true) {
                val sc = scheduledToCopy.getCompleted()
                if (sc.size > 0) {
                    triggerFileCopy(sc, sc.first(), backupperUI)
                }
            }
        }

        val hasRunningOp = (backupperUI.runningOp != null)

        if (scheduledToCopy?.isCompleted == true) {
            val sc = scheduledToCopy.getCompleted()

            sc.take(20).forEach { file ->
                if (hasRunningOp || !UISelection.enableGitBatch) ImGui.beginDisabled()
                if (ImGui.button("Copy##$file")) {
                    triggerFileCopy(sc, file, backupperUI)
                }
                if (hasRunningOp || !UISelection.enableGitBatch) ImGui.endDisabled()
                sameLine()

                val filesize = backupperUI.filesInfo.ifPresent { it.fileInfos?.get(file)?.size }
                val filesizeFormatted = filesize
                    ?.takeIf { it != "?" }
                    ?.let { String.format("%.2f", toGB(it.toLong())) + "GB" }
                    ?: "???"
                ImGui.text(filesizeFormatted)
                sameLine()

                ImGui.text(file)
            }
        }
        ImGui.end()
    }
}

private fun triggerFileCopy(sc: MutableList<String>, file: String, backupperUI: BackupperUI) = GlobalScope.launch {
    if (sc.contains(file)) {
        backupperUI.triggerCopy(file) {
            sc.remove(file)
        }
    }
}


fun showRepoInformationWindow() {
    ImGui.begin("Repo information")
    val shownRepo = selectedRepo
    if (shownRepo == null) {
        ImGui.textColored(RED, "Select repo first!")
    } else {
        ImGui.text("Repo: ${shownRepo.repo.root}")
        sameLine()
        if (ImGui.button("Refresh")) {
            shownRepo.refresh()
        }
        sameLine()
        ImGui.checkbox("Show Backupper", UISelection::showBackupperWindow)

        separator()

        if (!shownRepo.info.isCompleted) {
            ImGui.textColored(YELLOW, "Loading...")
        }

        tabBar("RepoSettings") {
            tabItem("Basic Info") {
                if (shownRepo.info.ifPresent { true } != true) {
                    textColored(RED, "Error reading repo!")
                }
                shownRepo.info.ifPresent { info ->
                    info.repositories.forEach { repo ->
                        showRepoDescriptorLine(
                            repo,
                            showBackupCheckbox = true,
                            showGroups = false
                        )
                    }
                }
            }

            tabItem("Groups") {
                shownRepo.info.ifPresent { info ->
                    info.repositories.forEach { repo ->
                        showRepoDescriptorLine(
                            repo,
                            showBackupCheckbox = false,
                            showGroups = true
                        )
                    }
                }
            }
        }
    }
    ImGui.end()
}

fun <T, R> Deferred<T?>?.ifPresent(block: (T) -> R): R? = takeIf { it?.isCompleted == true }?.getCompleted()?.let(block)

private fun showRepoDescriptorLine(repo: RepoItemUI, showBackupCheckbox: Boolean, showGroups: Boolean) {
    text(repo.size)
    sameLine()
    textColored(repo.displayColor, repo.description)
    sameLine()
    if (ImGui.button(repo.uuid)) {
        println("Opening explorer to ${repo.pathToNavigate}...")
        repo.pathToNavigate.ifPresent { pathToNavigate ->
            ProcessBuilder("explorer.exe", pathToNavigate).start()
        }
    }
    if (showBackupCheckbox) {
        sameLine()
        ImGui.checkbox("Is Backup##${repo.uuid}", object : MutableProperty<Boolean>() {
            override fun set(value: Boolean) {
                if (value) {
                    if (!selectedBackupRepoUUIDs.contains(repo.uuid)) {
                        selectedBackupRepoUUIDs.add(repo.uuid)
                    }
                } else {
                    selectedBackupRepoUUIDs.remove(repo.uuid)
                }
            }

            override fun get(): Boolean = repo.uuid in selectedBackupRepoUUIDs
        })
    }
    if (showGroups) {
        sameLine()
        repo.groups.ifPresent { groups ->
            groups.forEach { ImGui.text(it) }
        }
    }
}

class RepoItemUI(
    val uuid: String,
    val size: String,
    val displayColor: Vec4,
    val description: String,
    val pathToNavigate: Deferred<String?>,
    val groups: Deferred<List<String>>
)

class RepoUIInfo(info: RepositoriesInfoQueryResult, thisRepo: RepoUI, scope: CoroutineScope) {
    val repositories: List<RepoItemUI>

    init {
        repositories = (info.`trusted repositories` + info.`semitrusted repositories` + info.`untrusted repositories`)
            .map { repoInfo ->
                RepoItemUI(
                    uuid = repoInfo.uuid,
                    size = info.`annex sizes of repositories`.find { size -> size.uuid == repoInfo.uuid }?.size ?: "?",
                    displayColor = if (repoInfo.here) GREEN else GRAY,
                    description = repoInfo.description,
                    pathToNavigate = scope.async {
                        val info = thisRepo.remoteInfo(repoInfo.uuid).await()
                        if (repoInfo.here) {
                            thisRepo.repo.root.path
                        } else {
                            info?.`repository location`
                        }
                    },
                    groups = scope.async {
                        GitSimpleQuery(GitProcess(thisRepo.repo.root, "annex", "group", repoInfo.uuid))
                            .execute()
                            ?.split(" ")
                            ?: throw IllegalStateException("Could not get repo groups for ${thisRepo.repo.root}?!")
                    }
                )
            }
    }
}

class RepoUI(val repo: Repo) : Closeable {
    private var loadedInfo: Deferred<RepoUIInfo?>? = null
    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    val info: Deferred<RepoUIInfo?>
        get() {
            var info = loadedInfo
            if (info == null) {
                info = coroutineScope.async {
                    val data = GitJsonQuery(
                        GitProcess(repo.root, "annex", "info", "--fast", "--json"),
                        RepositoriesInfoQueryResult.serializer(),
                    ).execute()

                    data?.let { RepoUIInfo(it, this@RepoUI, coroutineScope) }
                }
                loadedInfo = info
            }
            return info
        }

    fun refresh() {
        loadedInfo = coroutineScope.async {
            val data = GitJsonQuery(
                GitProcess(repo.root, "annex", "info", "--json"),
                RepositoriesInfoQueryResult.serializer()
            ).execute()

            data?.let { RepoUIInfo(it, this@RepoUI, coroutineScope) }
        }
    }

    private val remotesInfo: MutableMap<String, Deferred<RemoteInfoQueryResult?>> = ConcurrentHashMap()

    fun remoteInfo(uuid: String): Deferred<RemoteInfoQueryResult?> =
        remotesInfo.computeIfAbsent(uuid) {
            coroutineScope.async {
                GitJsonQuery(
                    GitProcess(repo.root, "annex", "info", "--json", "--fast", uuid),
                    RemoteInfoQueryResult.serializer(),
                ).execute()
            }
        }

    override fun close() {
        coroutineScope.cancel()
    }
}

class GitProcess(repoRoot: File, vararg args: String) {
    val builder = ProcessBuilder("git", *args)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .directory(repoRoot)

    init {
        GitCommandHistory.record(builder)
    }

    fun onStart() {
        GitCommandHistory.changeState(builder, GitCommandState.RUNNING)
    }

    fun onFinish() {
        GitCommandHistory.changeState(builder, GitCommandState.FINISHED)
    }
}

typealias ProcessReader<T> = (Process) -> T?

open class GitQuery<T>(val process: GitProcess, val reader: ProcessReader<T>) {
    suspend fun execute(): T? {
        this.process.onStart()
        try {
            val process: Process = withContext(Dispatchers.IO) {
                process.builder.start()
            }

            withContext(Dispatchers.IO) {
                process.waitFor()
            }

            return reader(process)
        } finally {
            this.process.onFinish()
        }
    }
}

class GitSimpleQuery(gitProcess: GitProcess) : GitQuery<String>(gitProcess, { process ->
    if (process.exitValue() != 0) {
        null
    } else {
        BufferedReader(InputStreamReader(process.inputStream)).readText()
    }
})

class GitJsonQuery<T>(process: GitProcess, serializer: DeserializationStrategy<T>) :
    GitQuery<T>(process, serializer.fromJsonIfSuccessfulAndNonempty())

fun <T> DeserializationStrategy<T>.fromJsonIfSuccessfulAndNonempty(): ProcessReader<T?> {
    val strategy: DeserializationStrategy<T> = this
    return { process: Process ->
        if (process.exitValue() != 0) {
            System.err.println(BufferedReader(InputStreamReader(process.errorStream)).readText())
            null
        } else {
            val result = BufferedReader(InputStreamReader(process.inputStream)).readText()
            if (result.isEmpty()) {
                System.err.println("Process returned no result")
                null
            } else {
                jsonDecoder.decodeFromString(strategy, result)
            }
        }
    }
}

object UISelection {
    var selectedRepo: RepoUI? =
        AppSettings.repos.firstOrNull()?.let { File(it) }?.let { Repo(it) }?.let { RepoUI(it) }
        set(value) {
            selectedFile = null
            selectedBackupRepoUUIDs.clear()
            enableGitBatch = false
            enableAutoCopy = false
            field = value
        }

    var selectedFile: Repo.RepoFile? = null

    var showBackupperWindow: Boolean = true

    var enableGitBatch: Boolean = false
        set(value) {
            if (!value) currentBackupper?.close()
            field = value
        }
    var enableAutoCopy: Boolean = false
}

fun showGitExecutionStateWindow() {
    ImGui.begin("git-annex execution state")
    GitCommandHistory.tail(49).forEach {
        val icon = when (it.state) {
            GitCommandState.SCHEDULED -> "S"
            GitCommandState.RUNNING -> "R"
            GitCommandState.FINISHED -> "D"
        }
        ImGui.text("$icon ${shorten(it.cmd.command().toString())}")
    }
    if (GitCommandHistory.size > 49) {
        ImGui.text("...")
    }
    ImGui.end()
}

private fun shorten(it: String) =
    if (it.length < 100) {
        it
    } else {
        it.substring(0 until 97) + "..."
    }

fun showSelectedFileDetailsWindow() {
    ImGui.begin("File details")
    if (selectedFile != null) {
        ImGui.text(selectedFile!!.name)
        ImGui.separator()
        if (!selectedFile!!.whereis.isCompleted) {
            ImGui.text("Running whereis...")
        } else {
            selectedFile!!.whereis.ifPresent { that: WhereisQueryResult ->
                ImGui.text("Found ${that.whereis.size} locations.")
                that.whereis.forEach {
                    ImGui.text(it.uuid); sameLine()
                    ImGui.text(it.description); sameLine()
                    ImGui.text("#URLs ${it.urls.size}")
                }
            } ?: ImGui.text("Error reading whereis!")
        }
        separator()

        selectedFile!!.find.let {
            if (!it.isCompleted) {
                text("Running find...")
            } else {
                ImGui.text("File: ${it.getCompleted()?.file.toString()}")
                ImGui.text("Backend: ${it.getCompleted()?.backend.toString()}")
                ImGui.text("Bytesize: ${it.getCompleted()?.bytesize.toString()}")
            }
        }

        selectedFile!!.info.let {
            if (!it.isCompleted) {
                text("Running info...")
            } else {
                ImGui.text("Is Present: ${it.getCompleted()?.present.toString()}")
                ImGui.text("size: ${it.getCompleted()?.size.toString()}")
            }
        }
    }

    ImGui.end()

}

fun Repo.show(item: RepoItem) {
    when (item) {
        is Repo.RepoDir -> {
            dsl.treeNode(item.name) {
                item.subdirectories.forEach { show(it) }
                item.files.forEach { show(it) }
            }
        }

        is Repo.RepoFile -> {
            if (ImGui.button("i##${item.name}")) {
                selectedFile = item
            }
            sameLine()
            ImGui.text(item.name)
        }
    }
}

fun showSelectedRepoContentsWindow() {
    ImGui.begin("Repo contents")

    if (selectedRepo == null) {
        ImGui.text("Select repo!")
    } else {
        ImGui.text("Listing $selectedRepo:")
        selectedRepo!!.repo.let { it.show(it.RepoDir(it.root)) }
    }
    ImGui.end()
}

fun showSettingsWindow() {
    ImGui.begin("Settings")
    ImGui.text("Repos:")
    AppSettings.repos.forEach {
        if (ImGui.button(it)) {
            selectedRepo?.close()
            selectedRepo = RepoUI(Repo(File(it)))
        }
    }

    ImGui.checkbox(
        "Demo Window",
        ::showDemoWindow
    )             // Edit bools storing our window open/close state
    ImGui.end()
}

fun toGB(bytes: Long) =
    bytes.toFloat() / (1 shl 30)

val jsonDecoder = Json { ignoreUnknownKeys = true }