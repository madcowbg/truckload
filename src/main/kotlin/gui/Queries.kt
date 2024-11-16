package gui

import kotlinx.serialization.Serializable


//{ "command":"whereis",
//    "error-messages":[],
//    "file":"Movies/The Hobbit - The Cardinal Cut (Full).mp4",
//    "input":["Movies\\The Hobbit - The Cardinal Cut (Full).mp4"],
//    "key":"SHA256E-s6324584102--5197b1b31acb47b93f6f7160a998cf969dbf174bc3685cf00cdf5c3a83de3112.mp4",
//    "note":"2 copies\n\t18d7bf7b-70f0-4b14-86a7-c53d334bd581 -- Backups Vol.02/Videos [here]\n\t3dad22f3-41f0-48cb-ac9b-1b2b7affee54 -- bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [origin]\n",
//    "success":true,
//    "untrusted":[],
//    "whereis":[
//        { "description":"Backups Vol.02/Videos", "here":true, "urls":[], "uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581" },
//        { "description":"bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [origin]", "here":false, "urls":[], "uuid":"3dad22f3-41f0-48cb-ac9b-1b2b7affee54" }
//    ]
//}
@Serializable
data class WhereisLocation(val description: String, val here: Boolean, val urls: List<String>, val uuid: String)

@Serializable
data class WhereisQueryResult(val file: String, val whereis: List<WhereisLocation>)

//{"command":"info [TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv","error-messages":[],
//  "file":"[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv",
//  "input":["[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv"],
//  "key":"SHA256E-s2151158324--1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
//  "present":true,"size":"2.15 gigabytes",
//  "success":true}
@Serializable
data class FileInfoQueryResult(val file: String, val present: Boolean = false, val size: String = "?")

//{
// "annex sizes of repositories":[
//   {"description":"laptop/Videos","here":true,"size":"1.38 TB","uuid":"eb983149-36f1-4fb6-997b-8be094bc8581"},
//   {"description":"bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [nas/Videos]","here":false,"size":"1.38 TB","uuid":"3dad22f3-41f0-48cb-ac9b-1b2b7affee54"},
//   {"description":"Backups Vol.02/Videos [backups-vol-02-videos]","here":false,"size":"1.28 TB","uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581"}],
// "annexed files in working tree":5325,
// "available local disk space":"1.96 terabytes (+100 megabytes reserved)",
// "backend usage":{"SHA256E":5325},
// "bloom filter size":"32 mebibytes (1% full)",
// "combined annex size of all repositories":"4.04 terabytes",
// "command":"info",
// "error-messages":[],
// "input":[],
// "local annex keys":4887,
// "local annex size":"1.38 terabytes",
// "semitrusted repositories":[
//   {"description":"web","here":false,"uuid":"00000000-0000-0000-0000-000000000001"},
//   {"description":"bittorrent","here":false,"uuid":"00000000-0000-0000-0000-000000000002"},
//   {"description":"Backups Vol.02/Videos [backups-vol-02-videos]","here":false,"uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581"},
//   {"description":"bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [nas/Videos]","here":false,"uuid":"3dad22f3-41f0-48cb-ac9b-1b2b7affee54"},
//   {"description":"laptop/Videos","here":true,"uuid":"eb983149-36f1-4fb6-997b-8be094bc8581"}],
// "size of annexed files in working tree":"1.33 terabytes",
// "success":true,
// "temporary object directory size":"644.29 kilobytes",
// "transfers in progress":[],
// "trusted repositories":[],
// "untrusted repositories":[]}

//{"available local disk space":"1.96 terabytes (+100 megabytes reserved)",
// "command":"info",
// "error-messages":[],
// "input":[],
// "semitrusted repositories":[
// {"description":"web","here":false,"uuid":"00000000-0000-0000-0000-000000000001"},
// {"description":"bittorrent","here":false,"uuid":"00000000-0000-0000-0000-000000000002"},
// {"description":"Backups Vol.02/Videos [backups-vol-02-videos]","here":false,"uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581"},
// {"description":"bono.nonchev@4fb7a0458d7a:/git-annex-repos/Videos [nas/Videos]","here":false,"uuid":"3dad22f3-41f0-48cb-ac9b-1b2b7affee54"},
// {"description":"laptop/Videos","here":true,"uuid":"eb983149-36f1-4fb6-997b-8be094bc8581"}],
// "success":true,
// "transfers in progress":[],
// "trusted repositories":[],
// "untrusted repositories":[]}
@Serializable
data class RepositoriesInfoQueryResult(
    val `semitrusted repositories`: List<RepositoryDescription>,
    val success: Boolean,
    val `trusted repositories`: List<RepositoryDescription>, val `untrusted repositories`: List<RepositoryDescription>,
    val `annex sizes of repositories`: List<RepositoryAnnexSize> = emptyList(),
    val `size of annexed files in working tree`: String = "???",
    val `annexed files in working tree`: Long? = null
)

@Serializable
data class RepositoryDescription(val description: String, val here: Boolean, val uuid: String)

@Serializable
data class RepositoryAnnexSize(val description: String, val here: Boolean, val size: String, val uuid: String)


//{"available":"true",
// "command":"info backups-vol-02-videos",
// "cost":"100.0",
// "description":"Backups Vol.02/Videos [backups-vol-02-videos]",
// "error-messages":[],
// "input":["backups-vol-02-videos"],
// "last synced":"2024-11-09 00:17:05 UTC",
// "proxied":"false",
// "remote":"backups-vol-02-videos",
// "repository location":"N:\\Videos\\",
// "success":true,
// "trust":"semitrusted",
// "type":"git",
// "uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581"}

//{"available":"true",
// "command":"info backups-vol-02-videos",
// "cost":"100.0",
// "description":"Backups Vol.02/Videos [backups-vol-02-videos]",
// "error-messages":[],"input":["backups-vol-02-videos"],
// "last synced":"2024-11-09 00:17:05 UTC",
// "proxied":"false",
// "remote":"backups-vol-02-videos",
// "remote annex keys":4659,
// "remote annex size":"1.28 terabytes",
// "repository location":"N:\\Videos\\",
// "success":true,
// "trust":"semitrusted",
// "type":"git",
// "uuid":"18d7bf7b-70f0-4b14-86a7-c53d334bd581"}
@Serializable
data class RemoteInfoQueryResult(
    val available: Boolean? = null,
    val cost: Float? = null,
    val description: String,
    val proxied: Boolean? = null,
    val `remote annex keys`: Int = -1,
    val `remote annex size`: String = "?",
    val `repository location`: String? = null,
    val trust: String,
    val type: String? = null,
    val uuid: String,
    val `last synced`: String = "?" // fixme datetime
)

//{"backend":"SHA256E",
// "bytesize":"2151158324",
// "error-messages":[],
// "file":"[TorrentCounter.me].Thor.Ragnarok.2017.1080p.BluRay.x264.ESubs.mkv",
// "hashdirlower":"5ba\\979\\",
// "hashdirmixed":"5V\\PM\\",
// "humansize":"2.15 GB",
// "key":"SHA256E-s2151158324--1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
// "keyname":"1831c346f658fd08f943b5098793892b4b9ed0b83c7dd3b50104a0a13d3a7de3.mkv",
// "mtime":"unknown"}
@Serializable
data class FindQueryResult(val file: String, val bytesize: Long, val backend: String)
