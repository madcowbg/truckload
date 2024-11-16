package gui

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.serialization.builtins.nullable
import java.io.File
import java.util.concurrent.CompletableFuture

sealed interface RepoItem {
    val name: String
}

class Repo(val root: File) {
    inner class RepoFile(private var file: File) : RepoItem {
        override val name: String
            get() = file.name

        val whereis: Deferred<WhereisQueryResult?> by lazy {
            GlobalScope.async { // fixme
                Git.executeOnAnnex(root, WhereisQueryResult.serializer(), "whereis", "--json", file.relativeTo(root).path)
            }
        }

        val info: Deferred<FileInfoQueryResult?> by lazy {
            GlobalScope.async { // fixme
                Git.executeOnAnnex(root, FileInfoQueryResult.serializer(), "info", "--json", file.relativeTo(root).path)
            }
        }

        val find: Deferred<FindQueryResult?> by lazy {
            GlobalScope.async { // fixme
                Git.executeOnAnnex(root, FindQueryResult.serializer(), "find", "--json", file.relativeTo(root).path)
            }
        }
    }

    inner class RepoDir(private var dir: File) : RepoItem {
        val files: List<RepoFile> by lazy {
            dir.listFiles()?.filter { it.isFile }?.map { RepoFile(it) } ?: listOf()
        }

        val subdirectories: List<RepoDir> by lazy {
            dir.listFiles()?.filter { it.isDirectory }?.map { RepoDir(it) } ?: listOf()
        }

        override val name: String
            get() = dir.name
    }
}