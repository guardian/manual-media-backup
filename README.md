# manual-media-backup

## What is it?

At the Guardian, we maintain "nearline" copies of audio and video material on a couple of ObjectMatrix MatrixStore
appliances, hosted both on- and off-site.

Mostly, our asset management system takes care of copying media on and off, but we now have a need for a "manually"
driven method of copying media to and from the appliances.  That is where this app comes in.

## How to use it?

The app takes the form of a commandline program.  sbt provides us with a wrapper script, and for convenience we pack
the whole lot into Docker (though rpm or deb would be a simple modification to `build.sbt`).

When testing in Docker, you need to add in a `docker run` call to run the app.
So instead of: 
  `./manual-media-backup [options]`, you would run:
  `docker run --rm -v /path/to/mediaroot:/path/to/mediaroot guardianmultimedia/manual-media-backup:[version] [options]`
  
Commandline options are as follows:

>  --vault-file <value>     .vault file from ObjectMatrix Admin that describes the cluster, vault and login details. This is provided when you create the vault.

>  --lookup <value>         look up this filepath on the provided ObjectMatrix

>  -c, --copy-to-local <value>
                           set to a filepath to copy from the OM to a local file

>  -f, --copy-from-local <value>
                           set this to copy from a local file onto the OM

>  -s, --chunk-size <value>
                           set chunk size for transfer in Kb/s

>  -t, --checksum-type <value>
                           use the given checksum type (md5, sha-1, sha-256 etc.) or 'none' for no checksum

>  -l, --list <value>       read a list of files to backup from here. This could be a local filepath or an http/https URL.

So, for example:

```
./manual-media-backup --vault-file /Users/home/Desktop/my_vault_file.vault -l https://listserver.company.int/list/unimported?json=true
```

would take a list of files downloaded in NDJSON format from `https://listserver.company.int/list/unimported?json=true` and
copy them to the vault identified by `my_vault_file.vault`.

The .vault file is a text file containing vault IDs and credentials, and is provided by the ObjectMatrix Admin
program when creating a vault or a user.

## List file format

The underlying code supports two formats for list files:

 - a plain text, newline delimited list of file paths. These must resolve locally.
 - a list of objects as NDJSON (newline-delimited JSON), i.e. a collection of JSON objects printed one to a line.
 
The format is the same whether obtained from a local file or an HTTP(S) endpoint.  When using NDJSON, remember
that the source can't be pretty-printed!
The app expects the following fields to be present in the JSON list (see `src/main/scala/models/IncomingListEntry`):

```
{"filepath":"/path/to/files/directory","filename":"myfile.dat","mtime":"iso-encoded-timestamp","size":12345}
```
(an "iso-encoded-timestamp" should look like `2019-01-02T03:04:05.678Z` or similar. Anything that `ZonedDateTime.parse` can
read should suffice.)
Any other fields present will be ignored silently.

**At present the code for reading plain lists is not wired in**

### Why NDJSON?

We are going to be shifting multiple terabytes of data, and some form of progress monitoring is very useful when doing
these transfers that can last for days.  It's therefore necessary to get a bit of extra data, specifically the
size of each file, so that overall progress can be monitored.
When running incremental backups it's also important to see the modification time in order to work out whether a given
file needs to be backed up or not.  While CSV could provide this data, it's also risky in that a misplaced comma or
column confusion can break the code; JSON does not suffer from this as the fields are listed explicitly.
We retrieve the data lazily from a web stream from the `https://github.com/guardian/media-census.git` app, so it is
impractical to use a single JSON array object as that would require building and loading the entire stream first.
Instead, we efficiently stream individual JSON objects using Akka.

### A note on HTTPS certificates

The JVM is security-conscious and as a result requires a valid certificate in order to communicate in HTTPS.
If you're using public certs that's not a problem, but if you are using private certs issued by your organisation you
need to find a way to get the JVM to trust them.
Purposefully, it's easier to sort out a certificate than to disable hostname checking 
(admit it, that is what you were thinking of!)

You can provide a trusted HTTPS CA in the following way:

- download or otherwise obtain your organisation's public CA cert in binary, DER format
- run: `keytool -import -keystore /path/to/mykeystore.jks -file orgcert.cer` (the keytool app is provided by your JDK)
- add `-Djavax.net.ssl.trustStore=/path/to/mykeystore.jks` to the java options before running the app. Note that the property name is **case-sensitive**!
There are a couple of ways of doing this:
    - edit the `/usr/bin/manual-media-backup` wrapper script and add the entry to the `JAVA_OPTS` line.
    - set the `JAVA_OPTS` environment variable to the above string before running the app
    - In Docker, you'll need to make the keystore available to the app:

    ```
    docker run --rm -v path/to/mykeystore.jks:/etc/ssl/mykeystore.jks -e JAVA_OPTS=javax.net.ssl.trustStore=/etc    /ssl/mykeystore.jks -v /path/to/mediaroot:/path/to/mediaroot guardianmultimedia/manual-media-backup:[version] [options]
    ```

   will bind-mount the file from your local path into `/etc/ssl/mykeystore.jks` and set the `JAVA_OPTS` environment variable within the container's scope.

Still got trouble? Try Atlassian's SSLPoke (see https://confluence.atlassian.com/kb/unable-to-connect-to-ssl-services-due-to-pkix-path-building-failed-779355358.html)
to help you debug the issue.

## Running a backup
Copy all files from a list to a vault:

```
./manual-media-backup --vault-file /Users/home/Desktop/my_vault_file.vault -l https://listserver.company.int/list/unimported?json=true
```

Copy a single file from local to a vault:

```
./manual-media-backup --vault-file /Users/home/Desktop/my_vault_file.vault -f /path/to/somefile.bak
```

Copy a single file from a vault to local:

```
./manual-media-backup --vault-file /Users/home/Desktop/my_vault_file.vault -c /vault/path/file
```

## Still to do

- list-vault functionality
- bulk-copy from vault to local
