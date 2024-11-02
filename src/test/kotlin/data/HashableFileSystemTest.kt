package data

import data.repo.sql.FileSize
import data.storage.DeviceFileSystem
import data.storage.Hash
import data.storage.ReadonlyFileSystem
import org.junit.jupiter.api.Test
import perftest.DummyFileSystem
import perftest.TestDataSettings
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals


class HashedFile(val path: String, val size: FileSize, val hash: Hash, version: Long) {
    var version: Long = version
        private set

    fun bumpVersion(newVersion: Long) {
        check(newVersion > version)
        version = newVersion
    }

    override fun toString(): String = "HashedFile($version, \"$path\")"

}

interface HashCache {
    fun incrementVersion(): Long
    fun observedOn(version: Long, file: ReadonlyFileSystem.File)
    fun removeOldVersions(currentVersion: Long)
    fun exists(file: ReadonlyFileSystem.File): Boolean
    fun walk(): Sequence<HashedFile>
}

class InMemHashCache : HashCache {
    private val hashedFiles = hashMapOf<String, MutableMap<Hash, HashedFile>>()
    private val currentVersion: AtomicLong = AtomicLong(0)

    override fun incrementVersion(): Long = currentVersion.incrementAndGet()

    override fun observedOn(version: Long, file: ReadonlyFileSystem.File) {
        val filesAtPath = hashedFiles.computeIfAbsent(file.path) { mutableMapOf() }
        val existing = filesAtPath[file.hash]
        if (existing == null) {
            filesAtPath[file.hash] =
                HashedFile(
                    path = file.path,
                    size = FileSize(file.fileSize),
                    hash = file.hash,
                    version = version
                )
        } else {
            existing.bumpVersion(version)
        }
    }

    override fun removeOldVersions(currentVersion: Long) {
        hashedFiles.values.forEach { hashToFile ->
            // retain only newer files
            hashToFile.filter { it.value.version >= currentVersion }
        }
    }

    override fun exists(file: ReadonlyFileSystem.File): Boolean {
        val version = currentVersion.get()

        return hashedFiles[file.path]
            ?.let { it.any { (hash, hashedFile) -> file.hash == hash && hashedFile.version == version } }
            ?: false
    }

    override fun walk(): Sequence<HashedFile> {
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

        thisCache.walk().forEach { hashedFile ->
            if (!data.exists(hashedFile.path)) {
                yield(Difference("$hashedFile not in file system"))
            } else {
                val actualFile = data.resolve(hashedFile.path)
                if (actualFile.fileSize != hashedFile.size.value) {
                    yield(Difference("Size $hashedFile = ${hashedFile.size} != ${actualFile.fileSize}"))
                }
                if (actualFile.hash != hashedFile.hash) {
                    yield(Difference("Hash $hashedFile = ${hashedFile.hash} != ${actualFile.hash}"))
                }
            }
        }
    }
}

fun ReadonlyFileSystem.createHashCache(): HashCache =
    InMemHashCache().also { it.updateAllFrom(this) }

fun HashCache.updateAllFrom(fs: ReadonlyFileSystem) {
    val version = this.incrementVersion()
    fs.walk().forEach {
        this.observedOn(version, it)
    }
    this.removeOldVersions(version)
}

class HashableFileSystemTest {
    @Test
    fun `create hash for filesystem`() {
        val data: ReadonlyFileSystem =
            DeviceFileSystem("${TestDataSettings.test_path}/.experiments/data")

        val hashedFileSystem = data.createHashCache()

        hashedFileSystem.differences(data).forEach { println(it) }
        assertEquals(listOf(), hashedFileSystem.differences(data).toList())
    }


    @Test
    fun `removed file gets detected and fixed`() {
        val data = DummyFileSystem(50, 5000, 10000, 40)
        val hashCache = data.createHashCache()

        data.removeRandomFile()

        hashCache.differences(data).forEach { println(it) }
        assertEquals(1, hashCache.differences(data).count())
        assertEquals(
            listOf(Difference("HashedFile(1, \"joEcXUiqbFArkVsaeXszICvZcMpzLUTtQkmPihta\") not in file system")),
            hashCache.differences(data).toList()
        )
    }

    @Test
    fun `change file gets detected and fixed`() {
        val data = DummyFileSystem(50, 5000, 10000, 40)
        val hashCache = data.createHashCache()

        data.changeRandomFile()

        hashCache.differences(data).forEach { println(it) }
        assertEquals(3, hashCache.differences(data).count())
        assertEquals(
            listOf(
                Difference("DummyFile[1001, joEcXUiqbFArkVsaeXszICvZcMpzLUTtQkmPihta] not in cache!"),
                Difference("Size HashedFile(1, \"joEcXUiqbFArkVsaeXszICvZcMpzLUTtQkmPihta\") = FileSize(value=4388) != 1001"),
                Difference("Hash HashedFile(1, \"joEcXUiqbFArkVsaeXszICvZcMpzLUTtQkmPihta\") = bebddb082ccf5867d041388f1a786b33 != 3c701a337d6fffc6a5ce572edcbbac7b")
            ),
            hashCache.differences(data).toList()
        )
    }
}