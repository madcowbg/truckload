package gui

import glm_.vec4.Vec4
import gui.UISelection.selectedFile
import gui.UISelection.selectedRepo
import imgui.ImGui
import imgui.ImGui.sameLine
import imgui.ImGui.separator
import imgui.ImGui.text
import imgui.ImGui.textColored
import imgui.dsl
import java.awt.Color
import java.io.File
import java.util.concurrent.CompletableFuture

fun runMainUILoop() {
    showSettingsWindow()

    showSelectedRepoContentsWindow()

    showSelectedFileDetailsWindow()

    showGitExecutionStateWindow()

    showRepoInformationWindow()
}

val RED = Vec4(1f, .2f, .2f, 1f)
val GREEN = Vec4(.2f, 1f, .2f, 1f)
val GRAY = Vec4(.7f, .7f, .7f, 1f)

fun showRepoInformationWindow() {
    ImGui.begin("Repo information")
    ImGui.text("Repo: ${selectedRepo?.repo?.root}")
    selectedRepo?.info?.takeIf { it.isDone }?.get()?.let {
        if (!it.success) {
            textColored(RED, "Error reading repo!")
        } else {
            separator()
            it.`trusted repositories`.forEach { repo ->
                val color = if(repo.here) GREEN else GRAY
                textColored(color, repo.description); sameLine(); text(repo.uuid)
            }
            separator()
            it.`semitrusted repositories`.forEach { repo ->
                val color = if(repo.here) GREEN else GRAY
                textColored(color, repo.description); sameLine(); text(repo.uuid)
            }
            separator()
            it.`untrusted repositories`.forEach { repo ->
                val color = if(repo.here) GREEN else GRAY
                textColored(color, repo.description); sameLine(); text(repo.uuid)
            }
        }
    }
    ImGui.end()
}

class RepoUI(val repo: Repo) {
    val info: CompletableFuture<RepositoriesInfoQueryResult?> by lazy {
        Git.executeOnAnnex(repo.root, RepositoriesInfoQueryResult.serializer(), "info", "--fast", "--json")
    }
}

object UISelection {
    var selectedRepo: RepoUI? = RepoUI(Repo(File(AppSettings.repos.firstOrNull())))
    var selectedFile: Repo.RepoFile? = null
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
                ImGui.text(it.get()?.file.toString())
                ImGui.text(it.get()?.backend.toString())
                ImGui.text(it.get()?.bytesize.toString())
            }
        }

        selectedFile!!.info.let {
            if (!it.isDone) {
                text("Running info...")
            } else {
                ImGui.text(it.get()?.present.toString())
                ImGui.text(it.get()?.size.toString())
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
            selectedFile = null
        }
    }

    ImGui.checkbox(
        "Demo Window",
        ::showDemoWindow
    )             // Edit bools storing our window open/close state
    ImGui.end()
}