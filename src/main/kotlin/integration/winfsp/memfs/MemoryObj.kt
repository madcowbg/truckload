package integration.winfsp.memfs

import com.github.jnrwinfspteam.jnrwinfsp.api.FileAttributes
import com.github.jnrwinfspteam.jnrwinfsp.api.FileInfo
import com.github.jnrwinfspteam.jnrwinfsp.api.ReparsePoint
import com.github.jnrwinfspteam.jnrwinfsp.api.WinSysTime
import java.nio.file.Path
import java.util.*

abstract class MemoryObj(
    val parent: MemoryObj?,
    var path: Path,
    var securityDescriptor: ByteArray,
    reparsePoint: ReparsePoint?
) {
    val fileAttributes: MutableSet<FileAttributes> = EnumSet.noneOf(FileAttributes::class.java)

    var reparseData: ByteArray? = null
    var reparseTag: Int = 0

    var creationTime: WinSysTime
    var lastAccessTime: WinSysTime

    var lastWriteTime: WinSysTime

    var changeTime: WinSysTime

    var indexNumber: Long = 0

    init {
        val now = WinSysTime.now()
        this.creationTime = now
        this.lastAccessTime = now
        this.lastWriteTime = now
        this.changeTime = now

        if (reparsePoint != null) {
            this.reparseData = reparsePoint.data
            this.reparseTag = reparsePoint.tag
            fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT)
        }
    }

    val name: String?
        get() = if (path.nameCount > 0) path.fileName.toString() else null


    abstract val allocationSize: Int
    abstract val fileSize: Int

    @JvmOverloads
    fun generateFileInfo(filePath: String = path.toString()): FileInfo {
        val res = FileInfo(filePath)
        res.fileAttributes.addAll(fileAttributes)
        res.allocationSize = allocationSize.toLong()
        res.fileSize = fileSize.toLong()
        res.creationTime = creationTime
        res.lastAccessTime = lastAccessTime
        res.lastWriteTime = lastWriteTime
        res.changeTime = changeTime
        res.reparseTag = reparseTag
        res.indexNumber = indexNumber
        return res
    }

    fun touch() {
        val now = WinSysTime.now()
        lastAccessTime = now
        lastWriteTime = now
        changeTime = now
    }

    fun touchParent() {
        this.parent?.touch()
    }
}