
package data

import data.storage.readStoredFileVersions
import data.storage.DeviceFileSystem
import dumpToConsole
import perftest.TestDataSettings
import kotlin.test.Test


class FileTest {

    @Test
    fun `traverse project folder`() {
        val storedFiles = readStoredFileVersions(DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data"))

        storedFiles.dumpToConsole()
    }
}