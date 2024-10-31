package data.repo.sql

data class FileSize(private val value: Long) {
    @Deprecated("use value itself")
    fun toInt(): Int = value.toInt()
}