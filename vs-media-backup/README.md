#vs-media-backup

This utility is in development.

It is indended to take a list of files from asset management in an NDJSON stream
and to copy them to an ObjectMatrix destination while updating Vidispine.

## Usage

All parameters are fed in via environment variables:

- **VIDISPINE_BASE_URL** location to access vidispine, e.g. https://vidispine.myorg.internal
- **VIDISPINE_USER** username to log in to Vidispine. This needs access to update and duplicate shapes, files and items.
- **VIDISPINE_PASSWORD** password for username
- **CHUNK_SIZE** (default 1Mb) chunk size for streaming operations, in Kbytes.
- **DEST_STORAGE_ID** the Vidispine storage ID for the vault that is being copied to
- **VAULT_FILE** .vault credentials file for the vault that is being copied to

Once these variables are set, then the app can simply be run:

```
./vsmediabackup
```
