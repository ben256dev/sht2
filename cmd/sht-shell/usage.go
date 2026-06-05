package main

var mainUsageText = `usage:
  sht                       upload stdin to default shelf
  sht <shelf>               upload stdin to shelf
  sht --                    upload stdin to default shelf
  sht <shelf> --            upload stdin to shelf
  sht <command> [args...]

commands:
  cat [digest ...]          print blobs
  stat [digest ...]         show blob status
  release [digest ...]      release blobs
  list [fields]             list refs
  quota                     show quota usage
  shelf <command>           manage shelves
  alias <command>           manage aliases
  upload <command>          resumable uploads
  help [topic]              show help

digest commands read digests from stdin when none are given.`

var catUsageText = `usage:
  sht cat [digest ...]

Print blobs for whitespace-delimited digests.
When no digests are given as arguments, digests are read from stdin.`

var statUsageText = `usage:
  sht stat [digest ...]

Check blobs for whitespace-delimited digests.
When no digests are given as arguments, digests are read from stdin.`

var releaseUsageText = `usage:
  sht release [digest ...]

Release refs for whitespace-delimited digests.
When no digests are given as arguments, digests are read from stdin.`

var shelfUsageText = `usage:
  sht shelf list
  sht shelf create <name> <max> [pending-max]
  sht shelf rename <old> <new>
  sht shelf default <name>
  sht shelf delete <name> --force

Creating a non-default shelf enables multi-shelf mode.
After that, scoped commands use the shelf name as the first word:
  sht <shelf>
  sht <shelf> --
  sht <shelf> list [fields]
  sht <shelf> release [digest ...]`

var listUsageText = `usage:
  sht list [fields]

fields:
  d  digest
  s  size
  k  key id
  c  created at
  t  state
  a  all (default)
  h  help

examples:
  sht list
  sht list d
  sht list dt
  sht <shelf> list dt`

var uploadUsageText = `usage:
  sht manifest              create/resume upload from manifest JSON
  sht upload <id> <index>   upload chunk bytes
  sht status <id>           show upload status
  sht finalize <id>         finalize upload

Pipe the manifest JSON to stdin for 'manifest'.
Pipe chunk bytes to stdin for 'upload'.`

var aliasUsageText = `usage:
  sht alias ns list
  sht alias ns create <name>
  sht alias list <namespace> [prefix]
  sht alias get <namespace> <path>
  sht alias cat <namespace> <path>
  sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]
  sht alias history <namespace> <path>
  sht alias grant <namespace> <path> <user> <read|write|admin>
  sht alias revoke <namespace> <path> <user>

Aliases are versioned names for blobs. Grants apply recursively to a directory path.
Use / as the path when granting or revoking namespace-wide access.`

const (
	usageCatDigests     = "usage: sht cat [digest ...]"
	usageStatDigests    = "usage: sht stat [digest ...]"
	usageReleaseDigests = "usage: sht release [digest ...]"
	usageListFields     = "usage: sht list [fields]"
	usageQuota          = "usage: sht quota"
	usageShelfScoped    = "usage: sht <shelf> --"
	usageUploadManifest = "usage: sht manifest < manifest.json"
	usageUploadChunk    = "usage: sht upload <digest> <index>"
	usageUploadStatus   = "usage: sht status <digest>"
	usageUploadFinalize = "usage: sht finalize <digest>"
	usageUploadStdin    = "usage: sht --"

	usageShelfList      = "usage: sht shelf list"
	usageShelfCreate    = "usage: sht shelf create <name> <max> [pending-max]"
	usageShelfRename    = "usage: sht shelf rename <old> <new>"
	usageShelfDefault   = "usage: sht shelf default <name>"
	usageShelfDelete    = "usage: sht shelf delete <name> --force"
	usageShelfDefaultDo = "usage: sht shelf default ..."

	usageAliasNSCreate = "usage: sht alias ns create <name>"
	usageAliasNSList   = "usage: sht alias ns list"
	usageAliasList     = "usage: sht alias list <namespace> [prefix]"
	usageAliasGet      = "usage: sht alias get <namespace> <path>"
	usageAliasCat      = "usage: sht alias cat <namespace> <path>"
	usageAliasSet      = "usage: sht alias set <namespace> <path> <digest> [--expect <version>] [-m <message>]"
	usageAliasHistory  = "usage: sht alias history <namespace> <path>"
	usageAliasGrant    = "usage: sht alias grant <namespace> <path> <user> <read|write|admin>"
	usageAliasRevoke   = "usage: sht alias revoke <namespace> <path> <user>"
)

func usage() {
	printUsage("main", mainUsageText)
}

func catUsage() {
	printUsage("cat", catUsageText)
}

func statUsage() {
	printUsage("stat", statUsageText)
}

func releaseUsage() {
	printUsage("release", releaseUsageText)
}

func shelfUsage() {
	printUsage("shelf", shelfUsageText)
}

func listUsage() {
	printUsage("list", listUsageText)
}

func uploadUsage() {
	printUsage("upload", uploadUsageText)
}

func aliasUsage() {
	printUsage("alias", aliasUsageText)
}
