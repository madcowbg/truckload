
package data

import data.repo.Repo
import data.repo.readFolder
import data.storage.DeviceFileSystem
import dumpToConsole
import java.io.File
import kotlin.test.Test


class FileTest {

    @Test
    fun `traverse project folder`() {
        val repo: Repo = readFolder(DeviceFileSystem("./.experiments/data"))

        repo.dumpToConsole()
    }
}