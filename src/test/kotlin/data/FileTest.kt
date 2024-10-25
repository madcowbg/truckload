
package data

import data.repo.Repo
import data.repo.readFolder
import dumpToConsole
import java.io.File
import kotlin.test.Test


class FileTest {

    @Test
    fun `traverse project folder`() {
        val repo: Repo = readFolder(File("./.experiments/data"))

        repo.dumpToConsole()
    }
}