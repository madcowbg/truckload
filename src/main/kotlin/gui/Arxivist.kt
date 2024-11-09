package gui

import glm_.vec4.Vec4
import gln.checkError
import gln.glViewport
import imgui.ConfigFlag
import imgui.DEBUG
import imgui.ImGui
import imgui.classes.Context
import imgui.div
import imgui.dsl.button
import imgui.impl.gl.ImplGL3
import imgui.impl.glfw.ImplGlfw
import org.lwjgl.opengl.GL
import org.lwjgl.opengl.GL11.*
import org.lwjgl.system.Platform
import uno.gl.GlWindow
import uno.glfw.GlfwWindow
import uno.glfw.Hints
import uno.glfw.VSync
import uno.glfw.glfw

// Data
lateinit var gAppWindow: GlWindow
lateinit var implGlfw: ImplGlfw
lateinit var implGl3: ImplGL3

// Our state
// (we use static, which essentially makes the variable globals, as a convenience to keep the example code easy to follow)
var showDemoWindow = false
var showAnotherWindow = false
var clearColor = Vec4(0.45f, 0.55f, 0.6f, 1f)

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
    val glfwWindow = GlfwWindow(1920, 1080, "Arxivist")
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
    io.fonts.addFontFromFileTTF("c:\\Windows\\Fonts\\segoeui.ttf", 16.0f)

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
            runMainUILoop()

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

