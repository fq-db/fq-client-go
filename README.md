# fq-client-go
GoLang client for [FQ database](https://github.com/rom8726/fq)

## Usage

```go
client, err := fq.New("127.0.0.1:1945", time.Second, 8)
if err != nil {
    return err
}
defer client.Close()

value, err := client.Incr(ctx, fq.CappingKey{Key: "user_42", Capping: 60})
```

## Sharding

Use `fq.NewSharded` when one logical database is split across several fq instances.
The connection settings are shared by all shards, and the caller provides the shard
selection function.

```go
client, err := fq.NewSharded(
    []string{"fq-a.internal:1945", "fq-b.internal:1945"},
    time.Second,
    8,
    func(key string, shardCount int) int {
        h := fnv.New32a()
        _, _ = h.Write([]byte(key))

        return int(h.Sum32() % uint32(shardCount))
    },
    fq.WithToken(os.Getenv("FQ_TOKEN")),
)
if err != nil {
    return err
}
defer client.Close()
```

Counter and rate-limit commands are routed by key; quota commands are routed by
quota name. `MDel` is split across shards and returns results in the input order.
`FlushDB`, `Truncate`, `Inspect`, and streams fan out to every shard. `Scan` and
`PScan` walk shards with an opaque cursor returned by the client.

## Authentication

When the server has `network.auth` configured, pass the token with `fq.WithToken`. The
server derives the role from the token; the client only carries it.

```go
client, err := fq.New("127.0.0.1:1945", time.Second, 8,
    fq.WithToken(os.Getenv("FQ_TOKEN")),
)
```

The token is sent inline with the required `HELLO` handshake after the connection is
established and again after every reconnect, so pooled connections stay authenticated.
A rejected token fails construction with an error matching `fq.ErrAuthFailed`:

```go
if errors.Is(err, fq.ErrAuthFailed) {
    // wrong or revoked token
}
```

Commands the token's role does not cover return a `*fq.ProtocolError` with code
`fq.CodePermissionDenied`:

```go
var protocolErr *fq.ProtocolError
if errors.As(err, &protocolErr) && protocolErr.Code == fq.CodePermissionDenied {
    // authenticated, but not allowed to run this command
}
```

## TLS

`fq.WithTLS` takes file paths and mirrors the client side of the server's `tls` block:

```go
client, err := fq.New("fq.internal:1945", time.Second, 8,
    fq.WithToken(os.Getenv("FQ_TOKEN")),
    fq.WithTLS(fq.TLSConfig{
        CAFile:     "/etc/fq/ca.crt",
        ServerName: "fq.internal",
    }),
)
```

For mutual TLS, add the client keypair — required whenever the server sets
`client_ca_file`:

```go
fq.WithTLS(fq.TLSConfig{
    CAFile:     "/etc/fq/ca.crt",
    CertFile:   "/etc/fq/client.crt",
    KeyFile:    "/etc/fq/client.key",
    ServerName: "fq.internal",
})
```

| Field | Meaning |
|---|---|
| `CAFile` | trust anchor for the server certificate; system roots are used when empty |
| `CertFile` / `KeyFile` | client keypair for mutual TLS; must be set together |
| `ServerName` | expected name in the server certificate; defaults to the dialed host |
| `SkipVerify` | disables server certificate verification; local testing only |
| `MinVersion` | `1.2` (default) or `1.3` |

When certificates come from somewhere other than the filesystem, build the `*tls.Config`
yourself and pass it with `fq.WithTLSConfig`:

```go
fq.WithTLSConfig(&tls.Config{
    RootCAs:      pool,
    Certificates: []tls.Certificate{certificate},
    ServerName:   "fq.internal",
    MinVersion:   tls.VersionTLS13,
})
```

## Tests

```shell
make test
make test-integration
```

The integration suite builds a real fq server and runs the client against it. By default it
clones the server from GitHub; `FQ_SERVER_REPO` and `FQ_SERVER_REF` override the source, and
`FQ_SERVER_DIR` builds from a local checkout instead of cloning — useful when testing
against server changes that are not pushed yet:

```shell
FQ_SERVER_DIR=../fq make test-integration
```
