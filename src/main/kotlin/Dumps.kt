@file:OptIn(ExperimentalEncodingApi::class)

import data.repo.Repo
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

fun Repo.dumpToConsole() {
    println("DUMPING REPO")
    println("Files:")
    files.forEach { println("[${it.logicalPath}] ${it.hash}") }
    println("Storage:")
    storage.forEach { version -> println("${version.hash} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}