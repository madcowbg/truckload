package processes

import data.storage.Hash

class FullSetBlockData(dataBlocks: Map<Hash, ByteArray>, parityBlocks: Map<Hash, ByteArray>) {
    val blocksData: Map<Hash, ByteArray> = dataBlocks + parityBlocks

    operator fun get(dataBlockHash: Hash): ByteArray = checkNotNull(blocksData[dataBlockHash])
    operator fun contains(dataBlockHash: Hash): Boolean = dataBlockHash in blocksData
}