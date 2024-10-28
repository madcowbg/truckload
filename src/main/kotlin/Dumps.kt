import data.storage.StoredFileVersion

fun List<StoredFileVersion>.dumpToConsole() {
    println("DUMPING REPO")
    println("Storage:")
    forEach { version -> println("${version.hash} ${version.location} ${version.path}") }
    println("END DUMPING REPO")
}