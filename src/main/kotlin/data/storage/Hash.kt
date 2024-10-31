package data.storage

import java.security.MessageDigest

open class Hash(private val value: ByteArray) : Comparable<Hash> {

    @OptIn(ExperimentalStdlibApi::class)
    constructor(value: String) : this(value.hexToByteArray(HexFormat.Default))

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