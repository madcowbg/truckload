package data.storage

interface ReadonlyFileSystem {
    fun resolve(path: String): File
    fun walk(): Sequence<File>
    fun digest(path: String): Hash?
    fun existsWithHash(path: String): Boolean

    interface File {
        val location: ReadonlyFileSystem
        val hash: Hash
        val path: String
        val fileSize: Long
        fun dataInRange(from: Long, to: Long): ByteArray
    }
}
