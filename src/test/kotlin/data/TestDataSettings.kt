package data

object TestDataSettings {
    val test_path: String = System.getenv("TRUCKLOAD_TEST_STORAGE") ?: "./"
}