package data.storage

import java.security.MessageDigest

class Hash(private val value: ByteArray) : Comparable<Hash> {

    @OptIn(ExperimentalStdlibApi::class)
    private val stringified: String = value.toHexString(HexFormat.Default)

    val storeable: String
        get() = stringified

    override fun compareTo(other: Hash): Int = stringified.compareTo(other.stringified)
    override fun equals(other: Any?): Boolean = other is Hash && stringified.contentEquals(other.stringified)
    override fun hashCode(): Int = stringified.hashCode()

    override fun toString(): String = stringified

    companion object {
        private val md5 = MessageDigest.getInstance("md5")

        fun digest(data: ByteArray): Hash = Hash(md5.digest(data))
    }
}