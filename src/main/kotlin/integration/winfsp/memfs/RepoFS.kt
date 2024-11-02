package integration.winfsp.memfs

import com.github.jnrwinfspteam.jnrwinfsp.api.*
import com.github.jnrwinfspteam.jnrwinfsp.service.ServiceException
import com.github.jnrwinfspteam.jnrwinfsp.service.ServiceRunner
import com.github.jnrwinfspteam.jnrwinfsp.util.NaturalOrderComparator
import data.repo.sql.StoredRepo
import data.repo.sql.catalogue.CatalogueFileVersions
import jnr.ffi.Pointer
import org.jetbrains.exposed.sql.SqlExpressionBuilder.match
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.OutputStream
import java.io.PrintStream
import java.nio.file.Path
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import kotlin.io.path.isDirectory
import kotlin.io.path.pathString

class RepoFS @JvmOverloads constructor(val repo: StoredRepo, verbose: Boolean = true) : WinFspStubFS() {
    private val rootPath: Path = Path.of("\\").normalize()
    private val nextFileHandle: AtomicLong = AtomicLong(0)

    private var volumeLabel: String = "Thin Repository"

    private val verboseOut: PrintStream = if (verbose) System.out else PrintStream(OutputStream.nullOutputStream())

    private val lock = Object()

    private val rootObj = DirObj(
        null,
        rootPath,
        SecurityDescriptorHandler.securityDescriptorToBytes(ROOT_SECURITY_DESCRIPTOR),
        null
    )

    private val objects: Map<String, MemoryObj>

    init {
        objects = hashMapOf(rootPath.toString() to rootObj)
        transaction(repo.db) {
            CatalogueFileVersions.selectAll()
                .map { row -> Path.of(CatalogueFileVersions.path(row)).normalize() }
                .forEach { filePath ->
                    if (filePath.parent == null) {
                        verboseOut.println("Skipping $filePath as it is a root file.")
                        return@forEach
                    }

                    val path = filePath.parent
                    if (path.pathString.contains("\\")) {
                        verboseOut.println("Skipping $path as it is not a root subfolder.")
                        return@forEach
                    }

                    objects["\\" + path.pathString] = DirObj(
                        rootObj,
                        path,
                        SecurityDescriptorHandler.securityDescriptorToBytes(ROOT_SECURITY_DESCRIPTOR),
                        null
                    )
                }
        }
    }

    private val fsVolumeInfo: VolumeInfo
        get() = VolumeInfo(
            MAX_FILE_NODES * MAX_FILE_SIZE,
            (MAX_FILE_NODES - objects.size) * MAX_FILE_SIZE,
            this.volumeLabel
        )

    override fun getVolumeInfo(): VolumeInfo {
        verboseOut.println("== GET VOLUME INFO ==")
        return synchronized(lock) {
            fsVolumeInfo
        }
    }

    override fun setVolumeLabel(volumeLabel: String): VolumeInfo {
        verboseOut.printf("== SET VOLUME LABEL == %s%n", volumeLabel)
        synchronized(lock) {
            this.volumeLabel = volumeLabel
            return fsVolumeInfo
        }
    }

    @Throws(NTStatusException::class)
    override fun getSecurityByName(fileName: String): Optional<SecurityResult> {
        verboseOut.printf("== GET SECURITY BY NAME == %s%n", fileName)
        synchronized(lock) {
            val filePath = getPath(fileName)
            if (!hasObject(filePath)) return Optional.empty()

            val obj = getObject(filePath)
            val securityDescriptor = obj.securityDescriptor
            val info = obj.generateFileInfo()
            verboseOut.printf(
                "== GET SECURITY BY NAME RETURNED == %s %s%n",
                SecurityDescriptorHandler.securityDescriptorToString(securityDescriptor), info
            )
            return Optional.of(SecurityResult(securityDescriptor, EnumSet.copyOf(obj.fileAttributes)))
        }
    }

    @Throws(NTStatusException::class)
    override fun create(
        fileName: String,
        createOptions: Set<CreateOptions>,
        grantedAccess: Int,
        fileAttributes: Set<FileAttributes>,
        securityDescriptor: ByteArray,
        allocationSize: Long,
        reparsePoint: ReparsePoint
    ): OpenResult {
        verboseOut.printf(
            "== CREATE == %s co=%s ga=%X fa=%s sd=%s as=%d rp=%s%n",
            fileName, createOptions, grantedAccess, fileAttributes,
            SecurityDescriptorHandler.securityDescriptorToString(securityDescriptor), allocationSize, reparsePoint
        )
        synchronized(lock) {
            val filePath = getPath(fileName)
            // Check for duplicate file/folder
            if (hasObject(filePath)) throw NTStatusException(-0x3fffffcb) // STATUS_OBJECT_NAME_COLLISION


            // Ensure the parent object exists and is a directory
            val parent = getParentObject(filePath)

            if (objects.size >= MAX_FILE_NODES) throw NTStatusException(-0x3ffffd16) // STATUS_CANNOT_MAKE

            if (allocationSize > MAX_FILE_SIZE) throw NTStatusException(-0x3fffff81) // STATUS_DISK_FULL

            throw NTStatusException(-0x3ffffd16) // STATUS_CANNOT_MAKE
//
//            val obj: MemoryObj
//            if (createOptions.contains(CreateOptions.FILE_DIRECTORY_FILE)) obj =
//                DirObj(parent, filePath, securityDescriptor, reparsePoint)
//            else {
//                val file = FileObj(parent, filePath, securityDescriptor, reparsePoint)
//                file.allocationSize = Math.toIntExact(allocationSize)
//                obj = file
//            }
//
//            obj.fileAttributes.addAll(fileAttributes)
//            obj.indexNumber = nextIndexNumber++
//            putObject(obj)
//
//            val fh = getNextFileHandle()
//            val info = obj.generateFileInfo()
//            verboseOut.printf("== CREATE RETURNED == %d - %s%n", fh, info)
//            return OpenResult(fh, info)
        }
    }

    @Throws(NTStatusException::class)
    override fun open(
        fileName: String,
        createOptions: Set<CreateOptions>,
        grantedAccess: Int
    ): OpenResult {
        verboseOut.printf("== OPEN == %s co=%s ga=%X%n", fileName, createOptions, grantedAccess)
        synchronized(lock) {
            val filePath = getPath(fileName)
            val obj = getObject(filePath)

            val fh = getNextFileHandle()
            val info = obj.generateFileInfo()
            verboseOut.printf("== OPEN RETURNED == %d - %s%n", fh, info)
            return OpenResult(fh, info)
        }
    }

    @Throws(NTStatusException::class)
    override fun overwrite(
        ctx: OpenContext,
        fileAttributes: MutableSet<FileAttributes>,
        replaceFileAttributes: Boolean,
        allocationSize: Long
    ): FileInfo {
        verboseOut.printf(
            "== OVERWRITE == %s fa=%s replaceFA=%s as=%d%n",
            ctx.path, fileAttributes, replaceFileAttributes, allocationSize
        )
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val file = getFileObject(filePath)

            fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_ARCHIVE)
            if (replaceFileAttributes) file.fileAttributes.clear()
            file.fileAttributes.addAll(fileAttributes)

            file.allocationSize = Math.toIntExact(allocationSize)
            file.fileSize = 0

            val now = WinSysTime.now()
            file.lastAccessTime = now
            file.lastWriteTime = now
            file.changeTime = now

            val info = file.generateFileInfo()
            verboseOut.printf("== OVERWRITE RETURNED == %s%n", info)
            return info
        }
    }

    override fun cleanup(ctx: OpenContext, flags: Set<CleanupFlags>) {
        verboseOut.printf("== CLEANUP == %s cf=%s%n", ctx, flags)
        try {
            synchronized(lock) {
                val filePath = getPath(ctx.path)
                val memObj = getObject(filePath)

                if (flags.contains(CleanupFlags.SET_ARCHIVE_BIT) && memObj is FileObj) memObj.fileAttributes.add(
                    FileAttributes.FILE_ATTRIBUTE_ARCHIVE
                )

                val now = WinSysTime.now()

                if (flags.contains(CleanupFlags.SET_LAST_ACCESS_TIME)) memObj.lastAccessTime = now

                if (flags.contains(CleanupFlags.SET_LAST_WRITE_TIME)) memObj.lastWriteTime = now

                if (flags.contains(CleanupFlags.SET_CHANGE_TIME)) memObj.changeTime = now

                if (flags.contains(CleanupFlags.SET_ALLOCATION_SIZE) && memObj is FileObj) memObj.adaptAllocationSize(
                    memObj.fileSize
                )

//                if (flags.contains(CleanupFlags.DELETE)) {
//                    if (isNotEmptyDirectory(memObj)) return  // abort if trying to remove a non-empty directory
//
//                    removeObject(memObj.path)
//
//                    verboseOut.println("== CLEANUP DELETED FILE/DIR ==")
//                }
                verboseOut.println("== CLEANUP RETURNED ==")
            }
        } catch (e: NTStatusException) {
            // we have no way to pass an error status via cleanup
        }
    }

    override fun close(ctx: OpenContext) {
        verboseOut.printf("== CLOSE == %s%n", ctx)
    }

    @Throws(NTStatusException::class)
    override fun read(ctx: OpenContext, pBuffer: Pointer, offset: Long, length: Int): Long {
        verboseOut.printf("== READ == %s off=%d len=%d%n", ctx.path, offset, length)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val file = getFileObject(filePath)

            val bytesRead = file.read(pBuffer, offset, length)
            verboseOut.printf("== READ RETURNED == bytes=%d%n", bytesRead)
            return bytesRead.toLong()
        }
    }

    @Throws(NTStatusException::class)
    override fun write(
        ctx: OpenContext,
        pBuffer: Pointer,
        offset: Long,
        length: Int,
        writeToEndOfFile: Boolean,
        constrainedIo: Boolean
    ): WriteResult {
        verboseOut.printf(
            "== WRITE == %s off=%d len=%d writeToEnd=%s constrained=%s%n",
            ctx.path, offset, length, writeToEndOfFile, constrainedIo
        )
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val file = getFileObject(filePath)
            val bytesTransferred = if (constrainedIo) file.constrainedWrite(pBuffer, offset, length).toLong()
            else file.write(pBuffer, offset, length, writeToEndOfFile).toLong()

            val info = file.generateFileInfo()
            verboseOut.printf("== WRITE RETURNED == bytes=%d %s%n", bytesTransferred, info)
            return WriteResult(bytesTransferred, info)
        }
    }

    @Throws(NTStatusException::class)
    override fun flush(ctx: OpenContext?): FileInfo? {
        verboseOut.printf("== FLUSH == %s%n", ctx)
        synchronized(lock) {
            if (ctx == null) return null // whole volume is being flushed


            val filePath = getPath(ctx.path)
            val obj: MemoryObj = getFileObject(filePath)

            val info = obj.generateFileInfo()
            verboseOut.printf("== FLUSH RETURNED == %s%n", info)
            return info
        }
    }

    @Throws(NTStatusException::class)
    override fun getFileInfo(ctx: OpenContext): FileInfo {
        verboseOut.printf("== GET FILE INFO == %s%n", ctx)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val obj = getObject(filePath)

            val info = obj.generateFileInfo()
            verboseOut.printf("== GET FILE INFO RETURNED == %s%n", info)
            return info
        }
    }

    @Throws(NTStatusException::class)
    override fun setBasicInfo(
        ctx: OpenContext,
        fileAttributes: Set<FileAttributes>,
        creationTime: WinSysTime,
        lastAccessTime: WinSysTime,
        lastWriteTime: WinSysTime,
        changeTime: WinSysTime
    ): FileInfo {
        verboseOut.printf(
            "== SET BASIC INFO == %s fa=%s ct=%s ac=%s wr=%s ch=%s%n",
            ctx, fileAttributes, creationTime, lastAccessTime, lastWriteTime, changeTime
        )
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val obj = getObject(filePath)

            if (!fileAttributes.contains(FileAttributes.INVALID_FILE_ATTRIBUTES)) {
                obj.fileAttributes.clear()
                obj.fileAttributes.addAll(fileAttributes)
            }
            if (creationTime.get() != 0L) obj.creationTime = creationTime
            if (lastAccessTime.get() != 0L) obj.lastAccessTime = lastAccessTime
            if (lastWriteTime.get() != 0L) obj.lastWriteTime = lastWriteTime
            if (changeTime.get() != 0L) obj.changeTime = changeTime

            val info = obj.generateFileInfo()
            verboseOut.printf("== SET BASIC INFO RETURNED == %s%n", info)
            return info
        }
    }

    @Throws(NTStatusException::class)
    override fun setFileSize(ctx: OpenContext, newSize: Long, setAllocationSize: Boolean): FileInfo {
        verboseOut.printf("== SET FILE SIZE == %s size=%d setAlloc=%s%n", ctx.path, newSize, setAllocationSize)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val file = getFileObject(filePath)

            if (setAllocationSize) file.allocationSize = Math.toIntExact(newSize)
            else file.fileSize = Math.toIntExact(newSize)

            val info = file.generateFileInfo()
            verboseOut.printf("== SET FILE SIZE RETURNED == %s%n", info)
            return info
        }
    }

    @Throws(NTStatusException::class)
    override fun canDelete(ctx: OpenContext) {
        verboseOut.printf("== CAN DELETE == %s%n", ctx)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)

            if (isNotEmptyDirectory(memObj)) throw NTStatusException(-0x3ffffeff) // STATUS_DIRECTORY_NOT_EMPTY
            verboseOut.println("== CAN DELETE RETURNED ==")
        }
    }

    @Throws(NTStatusException::class)
    override fun rename(ctx: OpenContext, oldFileName: String, newFileName: String, replaceIfExists: Boolean) {
        verboseOut.printf("== RENAME == %s -> %s%n", oldFileName, newFileName)
//        synchronized(lock) {
//            val oldFilePath = getPath(oldFileName)
//            val newFilePath = getPath(newFileName)
//
//            if (hasObject(newFilePath) && oldFileName != newFileName) {
//                if (!replaceIfExists) throw NTStatusException(-0x3fffffcb) // STATUS_OBJECT_NAME_COLLISION
//
//
//                val newMemObj = getObject(newFilePath)
//                if (newMemObj is DirObj) throw NTStatusException(-0x3fffffde) // STATUS_ACCESS_DENIED
//            }
//
//            // Rename file or directory (and all existing descendants)
//            for (obj in List.copyOf(objects.values)) {
//                if (obj.path.startsWith(oldFilePath)) {
//                    val relativePath = oldFilePath.relativize(obj.path)
//                    val newObjPath = newFilePath.resolve(relativePath)
//                    val newObj = removeObject(obj.path)
//                    newObj!!.path = newObjPath
//                    putObject(newObj)
//                }
//            }
//            verboseOut.println("== RENAME RETURNED ==")
//        }
    }

    @Throws(NTStatusException::class)
    override fun getSecurity(ctx: OpenContext): ByteArray {
        verboseOut.printf("== GET SECURITY == %s%n", ctx)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)

            val securityDescriptor = memObj.securityDescriptor
            verboseOut.printf(
                "== GET SECURITY RETURNED == %s%n",
                SecurityDescriptorHandler.securityDescriptorToString(securityDescriptor)
            )
            return securityDescriptor
        }
    }

    @Throws(NTStatusException::class)
    override fun setSecurity(ctx: OpenContext, securityDescriptor: ByteArray) {
        verboseOut.printf(
            "== SET SECURITY == %s sd=%s%n",
            ctx,
            SecurityDescriptorHandler.securityDescriptorToString(securityDescriptor)
        )
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)
            memObj.securityDescriptor = securityDescriptor
            verboseOut.println("== SET SECURITY RETURNED ==")
        }
    }

    @Throws(NTStatusException::class)
    override fun readDirectory(
        ctx: OpenContext,
        pattern: String,
        marker: String?,
        consumer: Predicate<FileInfo>
    ) {
        var marker = marker
        verboseOut.printf("== READ DIRECTORY == %s pa=%s ma=%s%n", ctx.path, pattern, marker)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val dir = getDirObject(filePath)

            // only add the "." and ".." entries if the directory is not root
            if (dir.path != rootPath) {
                if (marker == null) if (!consumer.test(dir.generateFileInfo("."))) return
                if (marker == null || marker == ".") {
                    val parentDir = getParentObject(filePath)
                    if (!consumer.test(parentDir.generateFileInfo(".."))) return
                    marker = null
                }
            }

            val finalMarker = marker
            objects.values.stream()
                .filter { obj: MemoryObj ->
                    obj.parent != null &&
                            obj.parent.path == dir.path
                }
                .sorted(Comparator.comparing(MemoryObj::name, NATURAL_ORDER))
                .dropWhile { obj: MemoryObj -> isBeforeMarker(obj.name, finalMarker) }
                .map { obj: MemoryObj ->
                    obj.generateFileInfo(
                        obj.name!!
                    )
                }
                .takeWhile(consumer)
                .forEach { o: FileInfo? -> }
        }
    }

    @Throws(NTStatusException::class)
    override fun getDirInfoByName(parentDirCtx: OpenContext, fileName: String): FileInfo {
        verboseOut.printf("== GET DIR INFO BY NAME == %s / %s%n", parentDirCtx.path, fileName)
        synchronized(lock) {
            val parentDirPath = getPath(parentDirCtx.path)
            getDirObject(parentDirPath) // ensure parent directory exists

            val filePath = parentDirPath.resolve(fileName).normalize()
            val memObj = getObject(filePath)

            val info = memObj.generateFileInfo(memObj.name!!)
            verboseOut.printf("== GET DIR INFO BY NAME RETURNED == %s%n", info)
            return info
        }
    }

    @Throws(NTStatusException::class)
    override fun getReparsePointData(ctx: OpenContext): ByteArray {
        verboseOut.printf("== GET REPARSE POINT DATA == %s%n", ctx)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)

            if (!memObj.fileAttributes.contains(FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT)) throw NTStatusException(-0x3ffffd8b) // STATUS_NOT_A_REPARSE_POINT


            val reparseData = memObj.reparseData
            verboseOut.printf("== GET REPARSE POINT DATA RETURNED == %s%n", reparseData.contentToString())
            return reparseData!!
        }
    }

    @Throws(NTStatusException::class)
    override fun setReparsePoint(ctx: OpenContext, reparseData: ByteArray, reparseTag: Int) {
        verboseOut.printf(
            "== SET REPARSE POINT == %s rd=%s rt=%d%n",
            ctx, reparseData.contentToString(), reparseTag
        )
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)

            if (isNotEmptyDirectory(memObj)) throw NTStatusException(-0x3ffffeff) // STATUS_DIRECTORY_NOT_EMPTY


            memObj.reparseData = reparseData
            memObj.reparseTag = reparseTag
            memObj.fileAttributes.add(FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT)
        }
    }

    @Throws(NTStatusException::class)
    override fun deleteReparsePoint(ctx: OpenContext) {
        verboseOut.printf("== DELETE REPARSE POINT == %s%n", ctx)
        synchronized(lock) {
            val filePath = getPath(ctx.path)
            val memObj = getObject(filePath)

            if (!memObj.fileAttributes.contains(FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT)) throw NTStatusException(-0x3ffffd8b) // STATUS_NOT_A_REPARSE_POINT


            memObj.reparseData = null
            memObj.reparseTag = 0
            memObj.fileAttributes.remove(FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT)
        }
    }

    private fun isNotEmptyDirectory(dir: MemoryObj): Boolean {
        if (dir is DirObj) {
            for (obj in objects.values) {
                if (obj.path.startsWith(dir.path)
                    && obj.path != dir.path
                ) return true
            }
        }

        return false
    }

    private fun getPath(filePath: String): Path {
        return Path.of(filePath).normalize()
    }

    private fun getPathKey(filePath: Path): String {
        return Objects.toString(filePath, null)
    }

    private fun hasObject(filePath: Path): Boolean {
        return objects.containsKey(getPathKey(filePath))
    }

    @Throws(NTStatusException::class)
    private fun getObject(filePath: Path): MemoryObj {
        val obj = objects[getPathKey(filePath)]
        if (obj == null) {
            getParentObject(filePath) // may throw exception with different status code
            throw NTStatusException(-0x3fffffcc) // STATUS_OBJECT_NAME_NOT_FOUND
        }

        return obj
    }

    @Throws(NTStatusException::class)
    private fun getParentObject(filePath: Path): DirObj {
        val parentObj = objects[getPathKey(filePath.parent)] ?: throw NTStatusException(-0x3fffffc6)
        // STATUS_OBJECT_PATH_NOT_FOUND

        if (parentObj !is DirObj) throw NTStatusException(-0x3ffffefd) // STATUS_NOT_A_DIRECTORY


        return parentObj
    }

    @Throws(NTStatusException::class)
    private fun getFileObject(filePath: Path): FileObj {
        val obj = getObject(filePath) as? FileObj ?: throw NTStatusException(-0x3fffff46)
        // STATUS_FILE_IS_A_DIRECTORY


        return obj
    }

    @Throws(NTStatusException::class)
    private fun getDirObject(filePath: Path): DirObj {
        val obj = getObject(filePath) as? DirObj ?: throw NTStatusException(-0x3ffffefd)
        // STATUS_NOT_A_DIRECTORY


        return obj
    }

    private fun getNextFileHandle(): Long {
        var fh: Long
        do {
            fh = nextFileHandle.incrementAndGet()
        } while (fh == 0L || fh.toInt() == 0) // ensure we never get a 0 value, either in 32-bit or 64-bit arch

        return fh
    }

    companion object {
        private const val ROOT_SECURITY_DESCRIPTOR = "O:BAG:BAD:PAR(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;WD)"
        private val NATURAL_ORDER: Comparator<String?> = NaturalOrderComparator()
        private const val MAX_FILE_NODES: Long = 10240
        private const val MAX_FILE_SIZE = (16 * 1024 * 1024).toLong()

        private fun isBeforeMarker(name: String?, marker: String?): Boolean {
            return marker != null && NATURAL_ORDER.compare(name, marker) <= 0
        }
    }
}

@Throws(NTStatusException::class, ServiceException::class)
fun main(args: Array<String>) {
    var mountPoint: Path? = null
    if (args.isNotEmpty()) mountPoint = Path.of(args[0])

    val memFS = RepoFS(StoredRepo.connect("C:\\Users\\Bono\\.experiments\\test_collection\\.repo"), true)
    System.out.printf("Mounting %s ...%n", mountPoint ?: "")
    ServiceRunner.mountLocalDriveAsService(
        "RepositoryFS", memFS, mountPoint, MountOptions()
            .setDebug(true)
            .setCase(MountOptions.CaseOption.CASE_SENSITIVE)
            .setSectorSize(512)
            .setSectorsPerAllocationUnit(1)
            .setForceBuiltinAdminOwnerAndGroup(true)
    )
}