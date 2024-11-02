package processes

import data.parity.restoreBlock
import data.storage.Hash
import io.github.oshai.kotlinlogging.KLogger

class PartialParitySet(
    val dataBlocks: Map<Hash, ByteArray?>,
    val dataBlockIdxs: List<Hash>,
    val parityBlocks: Map<Hash, ByteArray?>,
    val parityPHash: Hash,
    // todo add raid types
) {
    fun restore(logger: KLogger): FullSetBlockData {
        check(dataBlockIdxs.containsAll(dataBlocks.keys)) { "bad indexes, ${dataBlocks.keys} !in $dataBlockIdxs!" }
        val providedParityBlockData = parityBlocks[parityPHash]
        val providedFileBlocksData = dataBlockIdxs.map { dataBlocks[it] }

        val missingBlocksCnt = providedFileBlocksData.count { it == null }

        return if (providedParityBlockData != null && missingBlocksCnt == 0) {
            logger.info { "All blocks with data available! Returning ..." }
            FullSetBlockData(
                dataBlocks.mapValues { checkNotNull(it.value) },
                parityBlocks.mapValues { checkNotNull(it.value) })
        } else if (providedParityBlockData == null && missingBlocksCnt == 0) {
            check(missingBlocksCnt == 0)
            logger.info { "Parity block missing, restoring ..." }

            // restore parity block
            val parityBlockData = restoreBlock(providedFileBlocksData.map { checkNotNull(it) })
            FullSetBlockData(
                dataBlocks.mapValues { checkNotNull(it.value) },
                parityBlocks.mapValues {
                    if (it.key == parityPHash) {
                        parityBlockData
                    } else checkNotNull(it.value)
                })
        } else if (providedParityBlockData != null && missingBlocksCnt == 1) {
            val blockWithoutData: Hash = dataBlockIdxs[providedFileBlocksData.indexOfFirst { it == null }]
            logger.debug { "Restoring data block $blockWithoutData ..." }

            // restore file data block
            val fileBlockData = restoreBlock(listOf(providedParityBlockData) + providedFileBlocksData.filterNotNull())
                .apply {
                    check(Hash.digest(this) == blockWithoutData) {
                        "Failed restore $blockWithoutData, got ${Hash.digest(this)} instead!"
                    }
                }
            FullSetBlockData(
                dataBlocks.mapValues {
                    if (it.key == blockWithoutData) {
                        fileBlockData
                    } else checkNotNull(it.value)
                },
                parityBlocks.mapValues {
                    checkNotNull(it.value)
                })
        } else {
            logger.error { "Unsupported case - providedParityBlockData = $providedParityBlockData and missingBlocksCnt = $missingBlocksCnt!" }
            throw IllegalStateException("Unsupported case - providedParityBlockData = $providedParityBlockData and missingBlocksCnt = $missingBlocksCnt!")
        }
    }
}