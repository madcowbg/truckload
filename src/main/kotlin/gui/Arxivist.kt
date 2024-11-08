package examples

import glm_.vec4.Vec4
import gln.checkError
import gln.glViewport
import gui.AppSettings
import imgui.*
import imgui.ImGui.sameLine
import imgui.ImGui.separator
import imgui.ImGui.text
import imgui.classes.Context
import imgui.demo.DemoWindow
import imgui.dsl.button
import imgui.dsl.treeNode
import imgui.impl.gl.ImplGL3
import imgui.impl.glfw.ImplGlfw
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import org.lwjgl.opengl.GL
import org.lwjgl.opengl.GL11.*
import org.lwjgl.system.Platform
import uno.gl.GlWindow
import uno.glfw.GlfwWindow
import uno.glfw.Hints
import uno.glfw.VSync
import uno.glfw.glfw
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

// Data
lateinit var gAppWindow: GlWindow
lateinit var implGlfw: ImplGlfw
lateinit var implGl3: ImplGL3

// Our state
// (we use static, which essentially makes the variable globals, as a convenience to keep the example code easy to follow)
var showDemoWindow = true
var showAnotherWindow = false
var clearColor = Vec4(0.45f, 0.55f, 0.6f, 1f)
var f = 0f
var counter = 0


//{ "command":"whereis",
//    "error-messages":[],
//    "file":"Movies/The Hobbit - The Cardinal Cut (Full).mp4",
//    "input":["Movies\\The Hobbit - The Cardinal Cut (Full).mp4"],
//    "key":"SHA256E-s6324584102--5197b1b31acb47b93f6f7160a998cf969dbf174bc3685cf00cdf5c3a83de3112.mp4",
//    "note":"2 copies\n\t18d7bf7b-70f0-4b14-86a7-c53d334bd581 -- Backups Vol.02/Videos [here]\n\t3dad22f3-41f0-48cb-ac9b-1b2b7affee54 -- bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [origin]\n",
//    "success":true, 
//    "untrusted":[],
//    "whereis":[
//        { "description":"Backups Vol.02/Videos", "here":true, "urls":[], "uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581" },
//        { "description":"bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [origin]", "here":false, "urls":[], "uuid":"3dad22f3-41f0-48cb-ac9b-1b2b7affee54" }
//    ]
//}

@Serializable
data class WhereisLocation(val description: String, val here: Boolean, val urls: List<String>, val uuid: String)

@Serializable
data class WhereisQueryResult(val whereis: List<WhereisLocation>)

//{"command":"info [TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv","error-messages":[],
//  "file":"[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv",
//  "input":["[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv"],
//  "key":"SHA256E-s2151158324--1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
//  "present":true,"size":"2.15 gigabytes",
//  "success":true}
@Serializable
data class InfoQueryResult(val file: String, val present: Boolean, val size: String)

//{"backend":"SHA256E",
// "bytesize":"2151158324",
// "error-messages":[],
// "file":"[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv",
// "hashdirlower":"5ba\\979\\",
// "hashdirmixed":"5V\\PM\\",
// "humansize":"2.15 GB",
// "key":"SHA256E-s2151158324--1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
// "keyname":"1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
// "mtime":"unknown"}
@Serializable
data class FindQueryResult(val file: String, val bytesize: Long, val backend: String)


sealed interface RepoItem {
    val name: String
}

enum class GitCommandState {
    SCHEDULED,
    RUNNING,
    FINISHED
}

data class GitCommandHistory(val cmd: ProcessBuilder, var state: GitCommandState = GitCommandState.SCHEDULED)

object Git {
    private val gitLock = Object()
    private val jsonDecoder = Json { ignoreUnknownKeys = true }

    val commands = mutableListOf<GitCommandHistory>()

    private fun runGitProcess(workdir: File, vararg args: String): CompletableFuture<Process> {
        val processBuilder = ProcessBuilder("git", *args)
            .directory(workdir)
        commands.add(GitCommandHistory(processBuilder))
        val result = CompletableFuture<Process>()
        Thread {
            synchronized(gitLock) {
                commands.filter { it.cmd == processBuilder }.forEach { it.state = GitCommandState.RUNNING }

                val process = processBuilder.start()
                process.waitFor()

                commands.filter { it.cmd == processBuilder }.forEach { it.state = GitCommandState.FINISHED }
                result.complete(process)
            }
        }.start()
        return result
    }

    private fun readProcessOutput(process: Process): String =
        BufferedReader(InputStreamReader(process.inputStream)).readText()

    fun <T> ask(root: File, strategy: DeserializationStrategy<T>, vararg args: String): CompletableFuture<T?> {
        return runGitProcess(root, "annex", *args).thenApply { process ->
            if (process.exitValue() != 0) {
                println(BufferedReader(InputStreamReader(process.errorStream)).readText())
                null
            } else {
                val result = readProcessOutput(process)
                jsonDecoder.decodeFromString(strategy, result)
            }
        }
    }
}

class Repo(val root: File) {
    inner class RepoFile(private var file: File) : RepoItem {
        override val name: String
            get() = file.name

        val whereis: CompletableFuture<WhereisQueryResult?> by lazy {
            Git.ask(root, WhereisQueryResult.serializer(), "whereis", "--json", file.relativeTo(root).path)
        }

        val info: CompletableFuture<InfoQueryResult?> by lazy {
            Git.ask(root, InfoQueryResult.serializer(), "info", "--json", file.relativeTo(root).path)
        }

        val find: CompletableFuture<FindQueryResult?> by lazy {
            Git.ask(root, FindQueryResult.serializer(), "find", "--json", file.relativeTo(root).path)
        }
    }

    inner class RepoDir(private var dir: File) : RepoItem {
        val files: List<RepoFile> by lazy {
            dir.listFiles()?.filter { it.isFile }?.map { RepoFile(it) } ?: listOf()
        }

        val subdirectories: List<RepoDir> by lazy {
            dir.listFiles()?.filter { it.isDirectory }?.map { RepoDir(it) } ?: listOf()
        }

        override val name: String
            get() = dir.name
    }
}

var selectedRepo: Repo? = Repo(File(AppSettings.repos.firstOrNull()))
var selectedFile: Repo.RepoFile? = null

// Main code
fun main() {

    glfw {
        errorCB = { error, description -> println("Glfw Error $error: $description") }
        init()
        hints.context {
            debug = DEBUG

            // Decide GL+GLSL versions
            when (Platform.get()) {
                // TODO Opengl_es2? GL ES 2.0 + GLSL 100
                Platform.MACOSX -> {    // GL 3.2 + GLSL 150
                    ImplGL3.data.glslVersion = 150
                    version = "3.2"
                    profile = Hints.Context.Profile.Core      // 3.2+ only
                    forwardComp = true  // Required on Mac
                }

                else -> {   // GL 3.0 + GLSL 130
                    ImplGL3.data.glslVersion = 130
                    version = "3.0"
                    //profile = core      // 3.2+ only
                    //forwardComp = true  // 3.0+ only
                }
            }
        }
    }

    // Create window with graphics context
    val glfwWindow = GlfwWindow(1280, 720, "Dear ImGui GLFW+OpenGL3 OpenGL example")
    gAppWindow = GlWindow(glfwWindow)
    gAppWindow.makeCurrent()
    glfw.swapInterval = VSync.ON   // Enable vsync

    // Setup Dear ImGui context
    val ctx = Context()
    val io = ctx.io
    io.configFlags /= ConfigFlag.NavEnableKeyboard  // Enable Keyboard Controls
    io.configFlags /= ConfigFlag.NavEnableGamepad   // Enable Gamepad Controls

    // Setup Dear ImGui style
    ImGui.styleColorsDark()
//        ImGui.styleColorsLight()

    // Setup Platform/Renderer backend
    implGlfw = ImplGlfw(gAppWindow, true)
    implGl3 = ImplGL3()

    // Load Fonts
    // - If no fonts are loaded, dear imgui will use the default font. You can also load multiple fonts and use ImGui::PushFont()/PopFont() to select them.
    // - AddFontFromFileTTF() will return the ImFont* so you can store it if you need to select the font among multiple.
    // - If the file cannot be loaded, the function will return a nullptr. Please handle those errors in your application (e.g. use an assertion, or display an error and quit).
    // - The fonts will be rasterized at a given size (w/ oversampling) and stored into a texture when calling ImFontAtlas::Build()/GetTexDataAsXXXX(), which ImGui_ImplXXXX_NewFrame below will call.
    // - Use '#define IMGUI_ENABLE_FREETYPE' in your imconfig file to use Freetype for higher quality font rendering.
    // - Read 'docs/FONTS.md' for more instructions and details.
    // - Remember that in C/C++ if you want to include a backslash \ in a string literal you need to write a double backslash \\ !
    //io.Fonts->AddFontDefault();
    //io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\segoeui.ttf", 18.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/DroidSans.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Roboto-Medium.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Cousine-Regular.ttf", 15.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/DroidSans.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/ProggyTiny.ttf", 10.0f);
    //ImFont* font = io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\ArialUni.ttf", 18.0f, nullptr, io.Fonts->GetGlyphRangesJapanese());
    //IM_ASSERT(font != nullptr);

    // Poll and handle events (inputs, window resize, etc.)
    // You can read the io.WantCaptureMouse, io.WantCaptureKeyboard flags to tell if dear imgui wants to use your inputs.
    // - When io.WantCaptureMouse is true, do not dispatch mouse input data to your main application.
    // - When io.WantCaptureKeyboard is true, do not dispatch keyboard input data to your main application.
    // - When io.WantCaptureMouse is true, do not dispatch mouse input data to your main application, or clear/overwrite your copy of the mouse data.
    // - When io.WantCaptureKeyboard is true, do not dispatch keyboard input data to your main application, or clear/overwrite your copy of the keyboard data.
    // Generally you may always pass all inputs to dear imgui, and hide them from your application based on those two flags.

    // Main loop
    // [JVM] This automatically also polls events, swaps buffers and gives a MemoryStack instance for the i-th frame
    gAppWindow.loop {

        // Start the Dear ImGui frame
        implGl3.newFrame()
        implGlfw.newFrame()

        ImGui.newFrame()

        // 1. Show the big demo window (Most of the sample code is in ImGui::ShowDemoWindow()! You can browse its code to learn more about Dear ImGui!).
        if (showDemoWindow)
            ImGui.showDemoWindow(::showDemoWindow)


        // 2. Show a simple window that we create ourselves. We use a Begin/End pair to create a named window.
        run {
            showSettingsWindow()

            showSelectedRepoContentsWindow()

            showSelectedFileDetailsWindow()

            showGitStateWindow()

            /*ImGui.begin("Hello, world!")                          // Create a window called "Hello, world!" and append into it.

            ImGui.text("This is some useful text.")                // Display some text (you can use a format strings too)
            ImGui.checkbox(
                "Demo Window",
                ::showDemoWindow
            )             // Edit bools storing our window open/close state
            ImGui.checkbox("Another Window", ::showAnotherWindow)

            ImGui.slider("float", ::f, 0f, 1f)   // Edit 1 float using a slider from 0.0f to 1.0f
            ImGui.colorEdit3("clear color", clearColor)           // Edit 3 floats representing a color

            if (ImGui.button("Button"))                           // Buttons return true when clicked (most widgets return true when edited/activated)
                counter++

            ImGui.text("counter = $counter")

            ImGui.text("Application average %.3f ms/frame (%.1f FPS)", 1_000f / ImGui.io.framerate, ImGui.io.framerate)

            ImGui.end()*/

            // 3. Show another simple window.
            if (showAnotherWindow) {
                // Pass a pointer to our bool variable (the window will have a closing button that will clear the bool when clicked)
                ImGui.begin("Another Window", ::showAnotherWindow)
                ImGui.text("Hello from another window!")
                button("Close Me") { //  this takes advantage of functional programming and pass directly a lambda as last parameter
                    showAnotherWindow = false
                }
                ImGui.end()
            }
        }

        // Rendering
        ImGui.render()
        glViewport(gAppWindow.framebufferSize)
        glClearColor(
            clearColor.x * clearColor.w,
            clearColor.y * clearColor.w,
            clearColor.z * clearColor.w,
            clearColor.w
        )
        glClear(GL_COLOR_BUFFER_BIT)

        implGl3.renderDrawData(ImGui.drawData!!)

        if (DEBUG)
            checkError("mainLoop")
    }

    implGl3.shutdown()
    implGlfw.shutdown()
    ctx.destroy()

    GL.destroy() // TODO -> uno
    gAppWindow.destroy()
    glfw.terminate()
}

fun showGitStateWindow() {
    ImGui.begin("git-annex state")
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

private fun showSelectedFileDetailsWindow() {
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
            treeNode(item.name) {
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
        ImGui.text("Listing ${selectedRepo}:")
        selectedRepo!!.let { it.show(it.RepoDir(it.root)) }
    }
    ImGui.end()
}

private fun showSettingsWindow() {
    ImGui.begin("Settings")
    ImGui.text("Repos:")
    AppSettings.repos.forEach {
        if (ImGui.button(it)) {
            selectedRepo = Repo(File(it))
            selectedFile = null
        }
    }

    ImGui.checkbox(
        "Demo Window",
        ::showDemoWindow
    )             // Edit bools storing our window open/close state
    ImGui.end()
}
