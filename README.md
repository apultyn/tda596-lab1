# HTTP file server and pub-sub broker

A Go implementation of [task.md](docs/task.md) and [extension.md](docs/extension.md),
using only the standard library. Both services share the same binary and port.
TCP listening and connections use `net`; `net/http` is used only to parse requests.

- `src/` contains the command and the server package.
- `tests/` contains the TCP integration tests.
- `docs/` contains the assignment and its diagrams.

## Build and run

Requires Go 1.20 or newer. From this directory:

```sh
go build -o http_server ./src
./http_server 8080
```

On Windows:

```powershell
go build -o http_server.exe ./src
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
with a body include a byte-accurate `Content-Length` and `Content-Type`. Headers use CRLF,
and HTTP/1.1 requests require a Host header. Request headers are limited to
64 KiB; reads and writes each have a 30-second deadline. Uploads stream to disk
without buffering the entire body in memory.

## Pub-sub requests

| Method and path | Success response |
| --- | --- |
| `POST /subscribe?topic=X` | `201 Created` with `{"id":N}` |
| `POST /unsubscribe?topic=X&id=N` | `204 No Content` |
| `POST /publish?topic=X` | `200 OK` after distributing the raw request body |
| `GET /events?topic=X&id=N` | `200 OK` with one message, or `204 No Content` after 15 seconds with no message |

Use the ID returned by subscribe in later requests. A successful poll returns the
original bytes as `application/octet-stream` with the topic's sequence number in
`X-Sequence-Number`. Issue another poll after each response. Unsubscribing wakes
an outstanding poll with `404 Not Found`.

Missing or blank topics, malformed IDs, and overlapping polls for the same
subscriber return `400 Bad Request`. Unknown subscribers return `404 Not Found`.
Methods unsupported by a route return `501 Not Implemented`.

```sh
curl -X POST 'http://localhost:8080/subscribe?topic=news'
# Use the returned ID in place of 1. This poll can run before the publish.
curl -i 'http://localhost:8080/events?topic=news&id=1'
# In another terminal:
curl --data-binary 'hello' 'http://localhost:8080/publish?topic=news'
curl -X POST 'http://localhost:8080/unsubscribe?topic=news&id=1'
```

## Concurrency and atomic updates

A semaphore permits at most **10 active request handlers**. A long poll releases
its slot while waiting for a message and reacquires one before sending a response.
Publishers and file requests can therefore run while subscribers wait.

The broker assigns sequence numbers per topic and enqueues each message in that
order. Each subscriber has a queue of 32 messages and receives publications made
during its subscription. A full queue skips the new message for that subscriber,
so sequence numbers may have gaps. Broker state lives in memory and resets on restart.

POST writes to a uniquely named temporary file in the destination directory.
Only a fully received and closed upload is renamed into place. A shared
read/write lock serializes publication against downloads, including on Windows.
GET therefore returns a complete old or new version; concurrent writers cannot
mix their contents. Incomplete uploads leave the previous file intact, and
temporary files are cleaned up and cannot be downloaded.

## Run in Docker

```sh
docker build -t http-file-broker .
docker run --rm -p 127.0.0.1:8080:8080 -v http-server-files:/public http-file-broker
```

The container serves files from `/public` and listens on port 8080 by default.
The named volume preserves files between runs. The server runs as UID 65532, so
any bind-mounted document root must be writable by that user for file uploads.

## Verification

```sh
go test ./...
go vet ./...
```

Tests use real TCP connections to cover all MIME types, binary data, HTTP error
responses, uploads, interrupted uploads, simultaneous readers and writers,
broker fan-out, ordered delivery, queue overflow, and long-poll concurrency.
Where the Go race detector is available:

```sh
go test -race ./...
```

The assignment's status definitions are described in
[RFC 1945, section 9](https://www.rfc-editor.org/rfc/rfc1945#section-9).
