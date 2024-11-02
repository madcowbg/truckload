package data.repo.sql

data class FileSize(internal val value: Long) {
    @Deprecated("use value itself")
    fun toInt(): Int = value.toInt()
}