package gui.git

import kotlinx.serialization.Serializable

//{"command":"copy",
// "error-messages":[],
// "file":"bulkexport\\VID_20230507_125125_001\\default_preview.mp4",
// "input":["bulkexport\\VID_20230507_125125_001\\default_preview.mp4"],
// "key":"SHA256E-s1660709755--d3abb4d455340d73215d1f98b0648c001fcffa96820b8f2f315480d279a8128a.mp4",
// "note":"from backups-vol-01-Insta360...",
// "success":true}
@Serializable
data class CopyCmdResult(
    val file: String,
    //@SerialName("error-messages") val errorMessages: List<Object>,
    val success: Boolean
)