package integration.winfsp.memfs

import com.github.jnrwinfspteam.jnrwinfsp.api.FileAttributes
import com.github.jnrwinfspteam.jnrwinfsp.api.ReparsePoint
import java.nio.file.Path

class DirObj(parent: DirObj?, path: Path, securityDescriptor: ByteArray, reparsePoint: ReparsePoint?) :
    MemoryObj(parent, path, securityDescriptor, reparsePoint) {
    init {
        fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_DIRECTORY)
    }

    override val allocationSize: Int
        get() = 0

    override val fileSize: Int
        get() = 0
}