package data.repo.sql

import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.or

object ParitySets : IntIdTable("parity_sets") {
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