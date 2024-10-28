
package data

import data.storage.DeviceFileSystem
import dumpToConsole
import perftest.TestDataSettings
import kotlin.test.Test


class FileTest {

    @Test
    fun `traverse project folder`() {
        val rootLocation = DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")
        val storedFiles = rootLocation.walk()

        storedFiles.dumpToConsole()
    }
}