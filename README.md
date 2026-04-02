usage: gscp.exe [ --disable-raw-terminal ] [ --from-index ] [ --index-only ]
                [ --local-output-filename-format LOCALOUTPUTFILENAMEFORMAT ]
                [ --log-console LOG_LEVEL ] [ --overwrite ] [ --to-index ]
                [ --unsecure ] [ --web-session-pool-limit WEBSESSIONPOOLLIMIT ]
                SOURCE DEST

Upload files into GlobalSearch archives as documents or download search results
as files.

positional arguments:
  source        source specification
  destination   destination specification

optional arguments:
  --disable-raw-terminal
                disable raw access to the terminal.  Responses to prompts for
                credentials will be unobscured.
  --from-index  source specification is an index; not an individual file
  --index-only  can only be used with --to-index.  Outputs the index without
                exporting the actual documents.
  --local-output-filename-format LOCALOUTPUTFILENAMEFORMAT
                When the source is a local index file whose filenames are
                gscp:// pseudo-URIs, use this format to name the local output
                filenames
  --log-console LOG_LEVEL
                enable logging for the console at the given level
  --overwrite   allow existing destination files to be overwritten.  If the
                destination is remote, then --to-index must also be specified
                and the destination must refer to a search. This search's
                prompts are populated with fields from the source and if a
                single existing destination document is found, it is deleted
                before the source is imported
  --to-index    destination specification is an index; not an individual file.
  --unsecure    use HTTP instead of HTTPS.  This is almost certainly a very bad
                idea.
  --web-session-pool-limit WEBSESSIONPOOLLIMIT
                specify a session limit for the web session pool.  By default,
                only one session is used at a time, but specifying this option
                allows multiple requests to execute concurrently


A specification can be one of the following:
        1. A filesystem path (on the local computer or a network
           location)
        2. A gscp pseudo-URI.

A gscp pseudo-URI is meant to resemble a remote host specification like
in scp and has the following format:

  [gscp://][username@]hostname[/api-path]:database[/archive[/sub-archive ...]]

where:
        username:       The GlobalSearch user name.  The password must
                        be specified within the configuration file or
                        given interactively.
        hostname:       The hostname of the GlobalSearch API server.
        api-path:       The path on the hostname to the GlobalSearch
                        API.  This only needs to be specified if it is
                        not /square9api.
        database:       The name of the GlobalSearch database.
        archive:        The name of the GlobalSearch archive.
        sub-archive:    Optional sub archive(s)

A gscp pseudo-URI can have query parameters starting with a question
mark ("?") and separated by ampersands ("&").  When a gscp pseudo-URI
is a source, then the parameters represent prompt or field values to a
search.  When a gscp pseudo-URI is a target, then the parameters are
field values.

# Examples

Upload a local file to a GlobalSearch archive:

```
gscp /home/user/MyFile.txt "gscp://Username@mysquare9.com:My Database/My Archive?Field 1=a&Field 2=b"
```

Upload a CSV of files to a GlobalSearch archive

```
gscp --from-index /home/user/index.csv "gscp://Username@mysquare9.com:My Database/My Archive"
```

Download GlobalSearch search results to a CSV:

```
gscp --to-index "gscp://Username@mysquare9.com:My Database/My Archive/My Search?Prompt 1=a&Prompt 2=b" /home/user/index.csv
```
