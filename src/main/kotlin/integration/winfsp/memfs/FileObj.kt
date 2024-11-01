package integration.winfsp.memfs

import com.github.jnrwinfspteam.jnrwinfsp.api.FileAttributes
import com.github.jnrwinfspteam.jnrwinfsp.api.NTStatusException
import com.github.jnrwinfspteam.jnrwinfsp.api.ReparsePoint
import com.github.jnrwinfspteam.jnrwinfsp.api.WinSysTime
import jnr.ffi.Pointer
import java.nio.file.Path
import kotlin.math.min

class FileObj(parent: DirObj?, path: Path, securityDescriptor: ByteArray, reparsePoint: ReparsePoint?) :
    MemoryObj(parent, path, securityDescriptor, reparsePoint) {
    private var data: ByteArray = ByteArray(0)

    @set:Synchronized
    @get:Synchronized
    override var fileSize: Int = 0
        set(fileSize) {
            val prevFileSize = fileSize

            if (fileSize < prevFileSize) {
                for (i in fileSize until prevFileSize) {
                    data[i] = 0.toByte()
                }
            } else if (fileSize > allocationSize) adaptAllocationSize(fileSize)

            field = fileSize
        }

    init {
        fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_ARCHIVE)
    }

    @get:Synchronized
    @set:Synchronized
    override var allocationSize: Int = data.size
        get() = data.size
        set(newAllocationSize) {
            if (newAllocationSize != field) {
                // truncate or extend the data buffer
                val newFileSize = min(fileSize.toDouble(), newAllocationSize.toDouble()).toInt()
                this.data = data.copyOf(newAllocationSize)
                this.fileSize = newFileSize
            }
        }

    @Synchronized
    fun adaptAllocationSize(fileSize: Int) {
        val units = (Math.addExact(fileSize, ALLOCATION_UNIT) - 1) / ALLOCATION_UNIT
        allocationSize = units * ALLOCATION_UNIT
    }

    @Synchronized
    @Throws(NTStatusException::class)
    fun read(buffer: Pointer, offsetL: Long, size: Int): Int {
        val offset = Math.toIntExact(offsetL)
        if (offset >= fileSize) throw NTStatusException(-0x3fffffef) // STATUS_END_OF_FILE

        val bytesToRead = min((fileSize - offset).toDouble(), size.toDouble()).toInt()
        buffer.put(0, data, offset, bytesToRead)

        setReadTime()

        return bytesToRead
    }

    @Synchronized
    fun write(buffer: Pointer, offsetL: Long, size: Int, writeToEndOfFile: Boolean): Int {
        var begOffset = Math.toIntExact(offsetL)
        if (writeToEndOfFile) begOffset = fileSize

        val endOffset = Math.addExact(begOffset, size)
        if (endOffset > fileSize) fileSize = endOffset

        buffer[0, data, begOffset, size]

        setWriteTime()

        return size
    }

    @Synchronized
    fun constrainedWrite(buffer: Pointer, offsetL: Long, size: Int): Int {
        val begOffset = Math.toIntExact(offsetL)
        if (begOffset >= fileSize) return 0

        val endOffset = min(fileSize.toDouble(), Math.addExact(begOffset, size).toDouble()).toInt()
        val transferredLength = endOffset - begOffset

        buffer[0, data, begOffset, transferredLength]

        setWriteTime()

        return transferredLength
    }

    private fun setReadTime() {
        lastAccessTime = WinSysTime.now()
    }

    private fun setWriteTime() {
        lastWriteTime = WinSysTime.now()
    }

    companion object {
        private const val ALLOCATION_UNIT = 512
    }
}