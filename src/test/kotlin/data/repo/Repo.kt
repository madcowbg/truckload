package data.repo

import data.storage.StoredFileVersion

class RepoFile(val logicalPath: String, val hash: ByteArray)
class Repo(val files: List<RepoFile>, val storage: List<StoredFileVersion>)