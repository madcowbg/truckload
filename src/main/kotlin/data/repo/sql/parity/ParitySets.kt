package data.repo.sql.parity

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.or

object ParitySets : Table("parity_sets") {
    val hash = text("hash").uniqueIndex()

    override val primaryKey: PrimaryKey = PrimaryKey(hash)

    val numDeviceBlocks = integer("num_device_blocks").check { it.greater(0) }
    val parityPHash = reference("parity_p_hash", ParityBlocks.hash)
    val parityQHash = optReference("parity_q_hash", ParityBlocks.hash)
    val parityType = enumeration("parity_type", ParityType::class).check("RAID6 requires parityQHash") {
        it.eq(ParityType.RAID6).and(parityQHash.isNotNull()).or(
            it.eq(ParityType.RAID5).and(
                parityQHash.isNull()
            )
        )
    }
}