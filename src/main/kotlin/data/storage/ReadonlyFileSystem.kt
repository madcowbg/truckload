package data.storage

interface ReadonlyFileSystem {
    fun resolve(path: String): File
    fun walk(): Sequence<File>

    interface File {
        val location: ReadonlyFileSystem
        val hash: Hash
        val path: String
        val fileSize: Long
        fun dataInRange(from: Long, to: Long): ByteArray
    }
}
