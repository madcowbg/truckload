package data

import data.repo.sql.parity.ParitySets.hash
import data.storage.DeviceFileSystem
import data.storage.Hash
import data.storage.ReadonlyFileSystem
import org.junit.jupiter.api.Test
import perftest.TestDataSettings
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals


class HashedFile(val path: String, version: Long) {

    var version: Long = version
        private set

    fun bumpVersion(newVersion: Long) {
        check(newVersion > version)
        version = newVersion
    }

}

class HashCache {
    private val hashedFiles = hashMapOf<String, MutableMap<Hash, HashedFile>>()
    private val currentVersion: AtomicLong = AtomicLong(0)

    fun incrementVersion(): Long = currentVersion.incrementAndGet()

    fun observedOn(version: Long, file: ReadonlyFileSystem.File) {
        val filesAtPath = hashedFiles.computeIfAbsent(file.path) { mutableMapOf() }
        val existing = filesAtPath[file.hash]
        if (existing == null) {
            filesAtPath[file.hash] = HashedFile(path = file.path, version = version)
        } else {
            existing.bumpVersion(version)
        }
    }

    fun removeOld(currentVersion: Long) {
        hashedFiles.values.forEach { hashToFile ->
            // retain only newer files
            hashToFile.filter { it.value.version >= currentVersion }
        }
    }

    fun exists(file: ReadonlyFileSystem.File): Boolean {
        val version = currentVersion.get()

        return hashedFiles[file.path]
            ?.let { it.any { (hash, hashedFile) -> file.hash == hash && hashedFile.version == version } }
            ?: false
    }

    fun walk(): Sequence<HashedFile> {
        val version = currentVersion.get()
        return hashedFiles.asSequence().flatMap { pathFileVersions ->
            pathFileVersions.value.values.asSequence().filter { it.version == version }
        }
    }
}

data class Difference(val msg: String)

fun HashCache.differences(data: ReadonlyFileSystem): Sequence<Difference> {
    val thisCache = this
    return sequence {
        data.walk().forEach { if (!thisCache.exists(it)) yield(Difference("$it not in cache!")) }

        thisCache.walk().forEach { if (!data.existsWithHash(it.path)) yield(Difference("$it not in file system")) }
    }
}

class HashableFileSystemTest {
    @Test
    fun `create hash for filesystem`() {
        val data: ReadonlyFileSystem =
            DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        val hashedFileSystem = HashCache()
        val version = hashedFileSystem.incrementVersion()
        data.walk().forEach {
            hashedFileSystem.observedOn(version, it)
        }
        hashedFileSystem.removeOld(version)

        hashedFileSystem.differences(data).forEach { println(it) }
        assertEquals(listOf(), hashedFileSystem.differences(data).toList())
    }
}