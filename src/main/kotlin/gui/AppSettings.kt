package gui

import java.nio.charset.Charset

object AppSettings {
    var repoFolderSetting: StringBuilder = StringBuilder()
    init {
        repoFolderSetting.append("N:\\Videos")
    }
    val repoFolder
        get() = repoFolderSetting.toString()
}