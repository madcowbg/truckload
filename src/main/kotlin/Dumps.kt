import data.storage.ReadonlyFileSystem

fun Sequence<ReadonlyFileSystem.File>.dumpToConsole() {
    println("DUMPING REPO")
    println("Storage:")
    forEach { version -> println("${version.hash} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}