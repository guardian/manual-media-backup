# showmxschecksum

This is a simple app to scan all files on an objectmatrix vault
and show the file path and checksum.

As with the other tools here, you'll need a .vault file to provide login credentials.
It is output from the MatrixStore Admin app when you create a vault or user credentials for a vault

## Usage

(depending on your shell, you may need to use `export` rather than `declare -x`)
```
declare -x VAULT_FILE={path-to-.vault-credentials}
./showmxschecksum 
```