package perftest

import data.repo.sql.StoredRepo
import data.repo.sql.listOfIssues
import data.repo.sql.naiveInitializeRepo
import java.util.logging.Level
import java.util.logging.Logger

fun main(args: Array<String>) {
    val repoPath = "${TestDataSettings.test_path}/.experiments/test_build_inmem/.repo"
    StoredRepo.delete(repoPath)

    Logger.getGlobal().log(Level.INFO, "connecting to new repo...")
    StoredRepo.init(repoPath)
    val storedRepo: StoredRepo = StoredRepo.connect(repoPath)

    Logger.getGlobal().log(Level.INFO, "creating dummy file system...")
    val location = DummyFileSystem(nFiles = 100000, meanSize = 400, stdSize = 800, filenameLength = 100)

    Logger.getGlobal().log(Level.INFO, "initializing repo...")
    storedRepo.naiveInitializeRepo(location) {msg -> Logger.getGlobal().log(Level.INFO, msg)}
    Logger.getGlobal().log(Level.INFO, "checking for issues...")

    storedRepo.listOfIssues().forEach { println(it.message) }
    Logger.getGlobal().log(Level.INFO, "done!")
}
