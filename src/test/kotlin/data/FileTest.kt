
package data

import data.repo.Repo
import data.repo.readFolder
import data.storage.DeviceFileSystem
import dumpToConsole
import perftest.TestDataSettings
import kotlin.test.Test


class FileTest {

    @Test
    fun `traverse project folder`() {
        val repo: Repo = readFolder(DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data"))

        repo.dumpToConsole()
    }
}