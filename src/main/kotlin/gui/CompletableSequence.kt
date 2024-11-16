package gui

class CompletableSequence<T>(dataSeq: Sequence<T>) : Sequence<T> {
    private val data = dataSeq.iterator()

    private var loadedData: Iterator<T>? = null

    override fun iterator(): Iterator<T> {
        return object : Iterator<T> {
            override fun hasNext(): Boolean = synchronized(this@CompletableSequence) {
                data.hasNext() || (loadedData?.hasNext() == true)
            }

            override fun next(): T = synchronized(this@CompletableSequence) {
                if (data.hasNext()) {
                    data.next()
                } else {
                    check(loadedData?.hasNext() == true)
                    checkNotNull(loadedData).next()
                }
            }
        }
    }

    fun makeEager() = synchronized(this@CompletableSequence) {
        check(loadedData == null)
        loadedData = Iterable { data }.toList().iterator()
    }
}

fun <T> Sequence<T>.completable(): CompletableSequence<T> = CompletableSequence(this)