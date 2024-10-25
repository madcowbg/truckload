@file:OptIn(ExperimentalEncodingApi::class)

import data.repo.Repo
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

val ByteArray.printable: String
    get() = Base64.encode(this)

fun Repo.dumpToConsole() {
    println("DUMPING REPO")
    println("Files:")
    files.forEach { println("[${it.logicalPath}] ${it.hash.printable}") }
    println("Storage:")
    storage.forEach { version -> println("${version.hash.printable} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}