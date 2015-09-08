# S3piegel

S3piegel is a tool to mirror two AWS S3 folders. It's a bit like running

```
aws s3 sync s3://source s3://dest
```

except that it'll preserve object metadata. As the `aws` tool, S3piegel operates
concurrently: while the listings of both the source and destination folders are
sequential, the actual copying of files is done in parallel.

S3piegel uses the copy function of S3, the data is therefore only transiting
inside AWS and not through your connection. It'll skip files that already exist
in the destination folder (comparing only the name for now), unless you tell it
not to.

## Getting it

S3piegel is a standard Go tool, and is "go-gettable":

```
go get github.com/abustany/s3piegel
```

## Using it

Simply run

```
s3piegel s3://source s3://dest
```

run with `-help` to see the list of all options.
