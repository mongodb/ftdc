# FTDC-utils

# Installation

```sh
# for the ftdc command
go get github.com/10gen/ftdc-utils/cmd/ftdc
# for the library
go get github.com/10gen/ftdc-utils
```

# Usage

```sh
# for general chunk info
ftdc metrics.TIMESTAMP
# write statistical output to a json file
ftdc metrics.TIMESTAMP --out out.json
# write the raw decoded chunks to a json file
ftdc metrics.TIMESTAMP --raw --out out.json
```
