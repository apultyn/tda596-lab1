# HTTP file server

A Go implementation of [task.md](task.md), using only the standard library.
TCP listening and connections use `net`; `net/http` is used only to parse requests.

## Build and run

Requires Go 1.20 or newer. From this directory:

```sh
go build -o http_server .
./http_server 8080
```

On Windows:

```powershell
go build -o http_server.exe .
.\http_server.exe 8080
```

The required first argument is a port from 1 through 65535. The server listens on
all available interfaces. It serves files relative to its current working
directory, or an optional existing document root:

```sh
./http_server 8080 ./public
```

## Requests

`GET /name.txt` returns that file's exact bytes. Supported extensions and types:

| Extension | Content-Type |
| --- | --- |
| `.html` | `text/html` |
| `.txt` | `text/plain` |
| `.gif` | `image/gif` |
| `.jpeg`, `.jpg` | `image/jpeg` |
| `.css` | `text/css` |

Extensions are case insensitive. Query strings do not affect the filename.
Directory listings and implicit index files are not provided.

`POST /name.txt` stores the **raw request body** at that path, creating the file
or replacing its previous contents. Parent directories must already exist.
Successful creation returns `201 Created`; replacement returns `200 OK`.
An empty body creates an empty file. HTTP/1.1 chunked uploads and
`Expect: 100-continue` are supported.

```sh
curl --data-binary @example.txt http://localhost:8080/saved.txt
curl http://localhost:8080/saved.txt
```

In Windows PowerShell, use `curl.exe` for these commands.

Missing files return `404 Not Found`. Unsupported extensions, malformed
requests, paths outside the document root, and other file errors return
`400 Bad Request`. All methods other than GET and POST return
`501 Not Implemented`.

Each connection handles one HTTP/1.0 or HTTP/1.1 request, then closes. Responses
include a byte-accurate `Content-Length` and `Content-Type`. Headers use CRLF,
and HTTP/1.1 requests require a Host header. Request headers are limited to
64 KiB; reads and writes each have a 30-second deadline. Uploads stream to disk
without buffering the entire body in memory.

## Concurrency and atomic updates

A semaphore permits at most **10 request goroutines**. Further connections wait
for an active handler to finish before receiving a handler of their own.

POST writes to a uniquely named temporary file in the destination directory.
Only a fully received and closed upload is renamed into place. A shared
read/write lock serializes publication against downloads, including on Windows.
GET therefore returns a complete old or new version; concurrent writers cannot
mix their contents. Incomplete uploads leave the previous file intact, and
temporary files are cleaned up and cannot be downloaded.

## Verification

```sh
go test ./...
go vet ./...
```

Tests use real TCP connections to cover all MIME types, binary data, HTTP error
responses, uploads, interrupted uploads, simultaneous readers and writers, and
the ten-client limit. Where the Go race detector is available:

```sh
go test -race ./...
```

The assignment's status definitions are described in
[RFC 1945, section 9](https://www.rfc-editor.org/rfc/rfc1945#section-9).
