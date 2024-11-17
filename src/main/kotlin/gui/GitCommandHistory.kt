package gui

enum class GitCommandState {
    SCHEDULED,
    RUNNING,
    FINISHED
}

data class GitCommandHistoryItem(val cmd: ProcessBuilder, var state: GitCommandState = GitCommandState.SCHEDULED)
object GitCommandHistory {
    private val commands = mutableListOf<GitCommandHistoryItem>()
    val size: Int
        get() = synchronized(commands) { commands.size }

    fun record(builder: ProcessBuilder) {
        synchronized(commands) { commands.add(GitCommandHistoryItem(builder)) }
    }

    fun changeState(processBuilder: ProcessBuilder?, state: GitCommandState) {
        synchronized(commands) { commands.filter { it.cmd == processBuilder }.forEach { it.state = state } }
    }

    fun tail(n: Int): List<GitCommandHistoryItem> =
        synchronized(commands) { commands.slice((commands.size - n).coerceAtLeast(0) until commands.size).reversed() }
}