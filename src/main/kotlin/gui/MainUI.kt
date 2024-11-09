package gui

import glm_.vec4.Vec4
import gui.UISelection.selectedFile
import gui.UISelection.selectedRepo
import imgui.ImGui
import imgui.ImGui.sameLine
import imgui.ImGui.separator
import imgui.ImGui.text
import imgui.ImGui.textColored
import imgui.MutableProperty
import imgui.dsl
import java.io.Closeable
import java.io.File
import java.util.concurrent.CompletableFuture

fun runMainUILoop() {
    showSettingsWindow()

    showSelectedRepoContentsWindow()

    showSelectedFileDetailsWindow()

    showGitExecutionStateWindow()

    showRepoInformationWindow()

    if(UISelection.showBackupperWindow) {
        showBackupperWindow()
    }
}

val RED = Vec4(1f, .2f, .2f, 1f)
val GREEN = Vec4(.2f, 1f, .2f, 1f)
val GRAY = Vec4(.7f, .7f, .7f, 1f)
val YELLOW = Vec4(.6f, .6f, .2f, 1f)

class BackupperUI(val repoRoot: File, val backupRepoUUIDs: List<String>) : Closeable {
    var repositoriesInfo: CompletableFuture<CopyOpRepositoriesInfo>? = null // = loadRepositoriesInfo(repoRoot, backupRepoUUIDs)
    var filesInfo: CompletableFuture<CopyOpFilesInRepositoryInfo>? = null // = loadFilesInRepositoryInfo(repositoriesInfo, repoRoot)
    var inAnyBackup: CompletableFuture<CopyOnBackupState>? = null // = analyzeBackupState(backupRepoUUIDs, filesInfo)
    var copyingOperation: CompletableFuture<Unit>? = null // = copyFilesRequiringBackup(repoRoot, backupRepoUUIDs, filesInfo, repositoriesInfo, inAnyBackup)

    override fun close() {
        repositoriesInfo?.cancel(true)
        filesInfo?.cancel(true)
        inAnyBackup?.cancel(true)
        copyingOperation?.cancel(true)
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
        backupperUI.backupRepoUUIDs.forEach {ImGui.text(it)}
        separator()

        if (backupperUI.filesInfo == null){
            ImGui.text("TODO START!")
        }
    }
    ImGui.end()
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

        if (!shownRepo.info.isDone) {
            ImGui.textColored(YELLOW, "Loading...")
        }

        shownRepo.info.takeIf { it.isDone }?.get()?.let {
            if (!it.success) {
                textColored(RED, "Error reading repo!")
            } else {
                it.`trusted repositories`.forEach { repo -> shownRepo.showRepoDescriptorLine(it, repo) }
                separator()
                it.`semitrusted repositories`.forEach { repo -> shownRepo.showRepoDescriptorLine(it, repo) }
                separator()
                it.`untrusted repositories`.forEach { repo -> shownRepo.showRepoDescriptorLine(it, repo) }
            }
        }
    }
    ImGui.end()
}

private fun RepoUI.showRepoDescriptorLine(it: RepositoriesInfoQueryResult, repo: RepositoryDescription) {
    val size = it.`annex sizes of repositories`.find { size -> size.uuid == repo.uuid }?.size ?: "?"
    text(size)
    val color = if (repo.here) GREEN else GRAY
    sameLine()
    textColored(color, repo.description)
    sameLine()
    if (ImGui.button(repo.uuid)) {
        val info = this.remoteInfo(repo.uuid).get()
        if (info != null) {
            val pathToNavigate = if (repo.here) {
                this.repo.root.path
            } else {
                info.`repository location`
            }
            println("Opening explorer to $pathToNavigate...")
            if (pathToNavigate != null) {
                ProcessBuilder("explorer.exe", pathToNavigate).start()
            }
        }
    }
    sameLine()
    ImGui.checkbox("Is Backup##${repo.uuid}", object: MutableProperty<Boolean>() {
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

class RepoUI(val repo: Repo) {
    private var loadedInfo: CompletableFuture<RepositoriesInfoQueryResult?>? = null

    val info: CompletableFuture<RepositoriesInfoQueryResult?>
        get() {
            var info = loadedInfo
            if (info == null) {
                info =
                    Git.executeOnAnnex(repo.root, RepositoriesInfoQueryResult.serializer(), "info", "--fast", "--json")
                loadedInfo = info
            }
            return info
        }

    fun refresh() {
        loadedInfo = Git.executeOnAnnex(repo.root, RepositoriesInfoQueryResult.serializer(), "info", "--json")
    }

    private val remotesInfo: MutableMap<String, CompletableFuture<RemoteInfoQueryResult?>> = mutableMapOf()

    fun remoteInfo(uuid: String): CompletableFuture<RemoteInfoQueryResult?> =
        remotesInfo.computeIfAbsent(uuid) {
            Git.executeOnAnnex(repo.root, RemoteInfoQueryResult.serializer(), "info", "--json", "--fast", uuid)
        }
}

object UISelection {
    var selectedRepo: RepoUI? = RepoUI(Repo(File(AppSettings.repos.firstOrNull())))
        set(value) {
            selectedFile = null
            selectedBackupRepoUUIDs.clear()
            field = value
        }

    var selectedFile: Repo.RepoFile? = null

    var showBackupperWindow: Boolean = true
}

fun showGitExecutionStateWindow() {
    ImGui.begin("git-annex execution state")
    Git.commands.reversed().forEach {
        val icon = when (it.state) {
            GitCommandState.SCHEDULED -> "S"
            GitCommandState.RUNNING -> "R"
            GitCommandState.FINISHED -> "D"
        }
        ImGui.text("$icon ${it.cmd.command()}")
    }
    ImGui.end()
}


fun showSelectedFileDetailsWindow() {
    ImGui.begin("File details")
    if (selectedFile != null) {
        ImGui.text(selectedFile!!.name)
        ImGui.separator()
        if (!selectedFile!!.whereis.isDone) {
            ImGui.text("Running whereis...")
        } else {
            selectedFile!!.whereis.resultNow()?.let { that ->
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
            if (!it.isDone) {
                text("Running find...")
            } else {
                ImGui.text("File: ${it.get()?.file.toString()}")
                ImGui.text("Backend: ${it.get()?.backend.toString()}")
                ImGui.text("Bytesize: ${it.get()?.bytesize.toString()}")
            }
        }

        selectedFile!!.info.let {
            if (!it.isDone) {
                text("Running info...")
            } else {
                ImGui.text("Is Present: ${it.get()?.present.toString()}")
                ImGui.text("size: ${it.get()?.size.toString()}")
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
            selectedRepo = RepoUI(Repo(File(it)))
        }
    }

    ImGui.checkbox(
        "Demo Window",
        ::showDemoWindow
    )             // Edit bools storing our window open/close state
    ImGui.end()
}