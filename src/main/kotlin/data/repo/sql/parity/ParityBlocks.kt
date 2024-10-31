package data.repo.sql.parity

import data.storage.Hash
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table

object ParityBlocks: Table("parity_blocks") {
    fun hash(it: ResultRow): Hash = Hash(it[hash])

    val hash = text("hash").uniqueIndex()
    val size = integer("size").check{it.greaterEq(0)}

    override val primaryKey = PrimaryKey(hash)
}