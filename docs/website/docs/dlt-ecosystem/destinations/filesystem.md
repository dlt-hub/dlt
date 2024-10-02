# Cloud storage and filesystem
The filesystem destination stores data in remote file systems and cloud storage services like **AWS S3**, **Google Cloud Storage**, or **Azure Blob Storage**. Underneath, it uses [fsspec](https://github.com/fsspec/filesystem_spec) to abstract file operations. Its primary role is to be used as a staging area for other destinations, but you can also quickly build a data lake with it.

:::tip
Please read the notes on the layout of the data files. Currently, we are receiving feedback on it. Please join our Slack (icon at the top of the page) and help us find the optimal layout.
:::

## Install dlt with filesystem

Install the dlt library with filesystem dependencies:

```sh
pip install "dlt[filesystem]"
```

This installs the `s3fs` and `botocore` packages.

:::caution

You may also install the dependencies independently. Try:
```sh
pip install dlt
pip install s3fs
```
so pip does not fail on backtracking.
:::

## Initialize the dlt project

Let's start by initializing a new dlt project as follows:
```sh
dlt init chess filesystem
```

:::note
This command will initialize your pipeline with chess as the source and AWS S3 as the destination.
:::

## Set up the destination and credentials

### AWS S3
The command above creates a sample `secrets.toml` and requirements file for an AWS S3 bucket. You can install those dependencies by running:
```sh
pip install -r requirements.txt
```

To edit the dlt credentials file with your secret info, open `.dlt/secrets.toml`, which looks like this:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
```

If you have your credentials stored in `~/.aws/credentials`, just remove the **[destination.filesystem.credentials]** section above, and dlt will fall back to your **default** profile in local credentials. If you want to switch the profile, pass the profile name as follows (here: `dlt-ci-user`):

```toml
[destination.filesystem.credentials]
profile_name="dlt-ci-user"
```

You can also specify an AWS region:

```toml
[destination.filesystem.credentials]
region_name="eu-central-1"
```

You need to create an S3 bucket and a user who can access that bucket. dlt does not create buckets automatically.

1. You can create the S3 bucket in the AWS console by clicking on "Create Bucket" in S3 and assigning the appropriate name and permissions to the bucket.
2. Once the bucket is created, you'll have the bucket URL. For example, if the bucket name is `dlt-ci-test-bucket`, then the bucket URL will be:

   ```text
   s3://dlt-ci-test-bucket
   ```

3. To grant permissions to the user being used to access the S3 bucket, go to IAM > Users, and click on “Add Permissions”.
4. Below you can find a sample policy that gives the minimum permission required by dlt to a bucket we created above. The policy contains permissions to list files in a bucket, get, put, and delete objects. **Remember to place your bucket name in the Resource section of the policy!**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DltBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:GetObjectAttributes",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::dlt-ci-test-bucket/*",
                "arn:aws:s3:::dlt-ci-test-bucket"
            ]
        }
    ]
}
```
5. To obtain the access and secret key for the user, go to IAM > Users and in the “Security Credentials”, click on “Create Access Key”, and preferably select “Command Line Interface” and create the access key.
6. Obtain the “Access Key” and “Secret Access Key” created that are to be used in "secrets.toml".

#### Using S3 compatible storage

To use an S3 compatible storage other than AWS S3, such as [MinIO](https://min.io/) or [Cloudflare R2](https://www.cloudflare.com/en-ca/developer-platform/r2/), you may supply an `endpoint_url` in the config. This should be set along with AWS credentials:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
endpoint_url = "https://<account_id>.r2.cloudflarestorage.com" # copy your endpoint URL here
```

#### Adding additional configuration

To pass any additional arguments to `fsspec`, you may supply `kwargs` and `client_kwargs` in the config as a **stringified dictionary**:

```toml
[destination.filesystem]
kwargs = '{"use_ssl": true, "auto_mkdir": true}'
client_kwargs = '{"verify": "public.crt"}'
```

### Google storage
Run `pip install "dlt[gs]"` which will install the `gcfs` package.

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`.
You'll see AWS credentials by default.
Use Google cloud credentials that you may know from [BigQuery destination](bigquery.md)
```toml
[destination.filesystem]
bucket_url = "gs://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```
:::note
Note that you can share the same credentials with BigQuery, replace the `[destination.filesystem.credentials]` section with a less specific one: `[destination.credentials]` which applies to both destinations.
:::

If you have default Google Cloud credentials in your environment (i.e., on cloud function), remove the credentials sections above and `dlt` will fall back to the available default.

Use **Cloud Storage** admin to create a new bucket. Then assign the **Storage Object Admin** role to your service account.

### Azure Blob Storage

Run `pip install "dlt[az]"` which will install the `adlfs` package to interface with Azure Blob Storage.

Edit the credentials in `.dlt/secrets.toml`, you'll see AWS credentials by default; replace them with your Azure credentials.

Two forms of Azure credentials are supported:

#### SAS token credentials

Supply storage account name and either SAS token or storage account key

```toml
[destination.filesystem]
bucket_url = "az://[your_container name]" # replace with your container name

[destination.filesystem.credentials]
# The storage account name is always required
azure_storage_account_name = "account_name" # please set me up!
# You can set either account_key or sas_token, only one is needed
azure_storage_account_key = "account_key" # please set me up!
azure_storage_sas_token = "sas_token" # please set me up!
```

If you have the correct Azure credentials set up on your machine (e.g., via Azure CLI),
you can omit both `azure_storage_account_key` and `azure_storage_sas_token` and `dlt` will fall back to the available default.
Note that `azure_storage_account_name` is still required as it can't be inferred from the environment.

#### Service principal credentials

Supply a client ID, client secret, and a tenant ID for a service principal authorized to access your container.

```toml
[destination.filesystem]
bucket_url = "az://[your_container name]" # replace with your container name

[destination.filesystem.credentials]
azure_client_id = "client_id" # please set me up!
azure_client_secret = "client_secret"
azure_tenant_id = "tenant_id" # please set me up!
```

:::caution
**Concurrent blob uploads**
`dlt` limits the number of concurrent connections for a single uploaded blob to 1. By default, `adlfs` that we use splits blobs into 4 MB chunks and uploads them concurrently, which leads to gigabytes of used memory and thousands of connections for larger load packages. You can increase the maximum concurrency as follows:
```toml
[destination.filesystem.kwargs]
max_concurrency=3
```
:::

### Local file system

If for any reason you want to have those files in a local folder, set up the `bucket_url` as follows (you are free to use `config.toml` for that as there are no secrets required):

```toml
[destination.filesystem]
bucket_url = "file:///absolute/path"  # three / for an absolute path
```

:::tip
For handling deeply nested layouts, consider enabling automatic directory creation for the local filesystem destination. This can be done by setting `kwargs` in `secrets.toml`:

```toml
[destination.filesystem]
kwargs = '{"auto_mkdir": true}'
```

Or by setting an environment variable:
```sh
export DESTINATION__FILESYSTEM__KWARGS = '{"auto_mkdir": true/false}'
```
:::

`dlt` correctly handles the native local file paths. Indeed, using the `file://` schema may not be intuitive, especially for Windows users.

```toml
[destination.unc_destination]
bucket_url = 'C:\a\b\c'
```

In the example above, we specify `bucket_url` using **TOML's literal strings** that do not require [escaping of backslashes](https://github.com/toml-lang/toml/blob/main/toml.md#string).

```toml
[destination.unc_destination]
bucket_url = '\\localhost\c$\a\b\c'  # UNC equivalent of C:\a\b\c

[destination.posix_destination]
bucket_url = '/var/local/data'  # absolute POSIX style path

[destination.relative_destination]
bucket_url = '_storage/data'  # relative POSIX style path
```

In the examples above, we define a few named filesystem destinations:
* **unc_destination** demonstrates a Windows UNC path in native form.
* **posix_destination** demonstrates a native POSIX (Linux/Mac) absolute path.
* **relative_destination** demonstrates a native POSIX (Linux/Mac) relative path. In this case, the `filesystem` destination will store files in the `$cwd/_storage/data` path, where **$cwd** is your current working directory.

`dlt` supports Windows [UNC paths with the file:// scheme](https://en.wikipedia.org/wiki/File_URI_scheme). They can be specified using **host** or purely as a **path** component.

```toml
[destination.unc_with_host]
bucket_url="file://localhost/c$/a/b/c"

[destination.unc_with_path]
bucket_url="file:////localhost/c$/a/b/c"
```

:::caution
Windows supports paths up to 255 characters. When you access a path longer than 255 characters, you'll see a `FileNotFound` exception.

To overcome this limit, you can use [extended paths](https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation?tabs=registry). `dlt` recognizes both regular and UNC extended paths.

```toml
[destination.regular_extended]
bucket_url = '\\?\C:\a\b\c'

[destination.unc_extended]
bucket_url='\\?\UNC\localhost\c$\a\b\c'
```
:::

### SFTP
Run `pip install "dlt[sftp]"` which will install the `paramiko` package alongside `dlt`, enabling secure SFTP transfers.

Configure your SFTP credentials by editing the `.dlt/secrets.toml` file. By default, the file contains placeholders for AWS credentials. You should replace these with your SFTP credentials.

Below are the possible fields for SFTP credentials configuration:

```text
sftp_port             # The port for SFTP, defaults to 22 (standard for SSH/SFTP)
sftp_username         # Your SFTP username, defaults to None
sftp_password         # Your SFTP password (if using password-based auth), defaults to None
sftp_key_filename     # Path to your private key file for key-based authentication, defaults to None
sftp_key_passphrase   # Passphrase for your private key (if applicable), defaults to None
sftp_timeout          # Timeout for establishing a connection, defaults to None
sftp_banner_timeout   # Timeout for receiving the banner during authentication, defaults to None
sftp_auth_timeout     # Authentication timeout, defaults to None
sftp_channel_timeout  # Channel timeout for SFTP operations, defaults to None
sftp_allow_agent      # Use SSH agent for key management (if available), defaults to True
sftp_look_for_keys    # Search for SSH keys in the default SSH directory (~/.ssh/), defaults to True
sftp_compress         # Enable compression (can improve performance over slow networks), defaults to False
sftp_gss_auth         # Use GSS-API for authentication, defaults to False
sftp_gss_kex          # Use GSS-API for key exchange, defaults to False
sftp_gss_deleg_creds  # Delegate credentials with GSS-API, defaults to True
sftp_gss_host         # Host for GSS-API, defaults to None
sftp_gss_trust_dns    # Trust DNS for GSS-API, defaults to True
```

:::info
For more information about credentials parameters: https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect
:::

### Authentication methods

SFTP authentication is attempted in the following order of priority:

1. **Key-based authentication**: If you provide a `key_filename` containing the path to a private key or a corresponding OpenSSH public certificate (e.g., `id_rsa` and `id_rsa-cert.pub`), these will be used for authentication. If the private key requires a passphrase, you can specify it via `sftp_key_passphrase`. If your private key requires a passphrase to unlock, and you’ve provided one, it will be used to attempt to unlock the key.

2. **SSH Agent-based authentication**: If `allow_agent=True` (default), Paramiko will look for any SSH keys stored in your local SSH agent (such as `id_rsa`, `id_dsa`, or `id_ecdsa` keys stored in `~/.ssh/`).

3. **Username/Password authentication**: If a password is provided (`sftp_password`), plain username/password authentication will be attempted.

4. **GSS-API authentication**: If GSS-API (Kerberos) is enabled (`sftp_gss_auth=True`), authentication will use the Kerberos protocol. GSS-API may also be used for key exchange (`sftp_gss_kex=True`) and credential delegation (`sftp_gss_deleg_creds=True`). This method is useful in environments where Kerberos is set up, often in enterprise networks.


#### 1. Key-based authentication

If you use an SSH key instead of a password, you can specify the path to your private key in the configuration.

```toml
[destination.filesystem]
bucket_url = "sftp://[hostname]/[path]"
file_glob = "*"

[destination.filesystem.credentials]
sftp_username = "foo"
sftp_key_filename = "/path/to/id_rsa"     # Replace with the path to your private key file
sftp_key_passphrase = "your_passphrase"   # Optional: passphrase for your private key
```

#### 2. SSH agent-based authentication

If you have an SSH agent running with loaded keys, you can allow Paramiko to use these keys automatically. You can omit the password and key fields if you're relying on the SSH agent.

```toml
[destination.filesystem]
bucket_url = "sftp://[hostname]/[path]"
file_glob = "*"

[destination.filesystem.credentials]
sftp_username = "foo"
sftp_key_passphrase = "your_passphrase"   # Optional: passphrase for your private key
```
The loaded key must be one of the following types stored in ~/.ssh/: id_rsa, id_dsa, or id_ecdsa.

#### 3. Username and password authentication

This is the simplest form of authentication, where you supply a username and password directly.

```toml
[destination.filesystem]
bucket_url = "sftp://[hostname]/[path]"  # The hostname of your SFTP server and the remote path
file_glob = "*"                          # Pattern to match the files you want to upload/download

[destination.filesystem.credentials]
sftp_username = "foo"                    # Replace "foo" with your SFTP username
sftp_password = "pass"                   # Replace "pass" with your SFTP password
```


### Notes:
- **Key-based authentication**: Make sure your private key has the correct permissions (`chmod 600`), or SSH will refuse to use it.
- **Timeouts**: It's important to adjust timeout values based on your network conditions to avoid connection issues.

This configuration allows flexible SFTP authentication, whether you're using passwords, keys, or agents, and ensures secure communication between your local environment and the SFTP server.

## Write disposition
The filesystem destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to the dataset folder
- `replace` - all files that belong to such tables are deleted from the dataset folder, and then the current set of files is added.
- `merge` - falls back to `append`

### 🧪 Merge with delta table format
The [`upsert`](../../general-usage/incremental-loading.md#upsert-strategy) merge strategy is supported when using the [`delta`](#delta-table-format) table format.

:::caution
The `upsert` merge strategy for the `filesystem` destination with `delta` table format is considered experimental.
:::

```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="my_primary_key",
    table_format="delta"
)
def my_upsert_resource():
    ...
...
```

#### Known limitations
- `hard_delete` hint not supported
- Deleting records from nested tables not supported
  - This means updates to JSON columns that involve element removals are not propagated. For example, if you first load `{"key": 1, "nested": [1, 2]}` and then load `{"key": 1, "nested": [1]}`, then the record for element `2` will not be deleted from the nested table.

## File compression

The filesystem destination in the dlt library uses `gzip` compression by default for efficiency, which may result in the files being stored in a compressed format. This format may not be easily readable as plain text or JSON Lines (`jsonl`) files. If you encounter files that seem unreadable, they may be compressed.

To handle compressed files:

- To disable compression, you can modify the `data_writer.disable_compression` setting in your "config.toml" file. This can be useful if you want to access the files directly without needing to decompress them. For example:

```toml
[normalize.data_writer]
disable_compression=true
```

- To decompress a `gzip` file, you can use tools like `gunzip`. This will convert the compressed file back to its original format, making it readable.

For more details on managing file compression, please visit our documentation on performance optimization: [Disabling and enabling file compression](../../reference/performance#disabling-and-enabling-file-compression).

## Files layout
All the files are stored in a single folder with the name of the dataset that you passed to the `run` or `load` methods of the `pipeline`. In our example chess pipeline, it is **chess_players_games_data**.

:::note
Object storages are, in fact, key-blob storage, so the folder structure is emulated by splitting file names into components by a separator (`/`).
:::

You can control the files layout by specifying the desired configuration. There are several ways to do this.

### Default layout

Current default layout: `{table_name}/{load_id}.{file_id}.{ext}`

:::note
The default layout format has changed from `{schema_name}.{table_name}.{load_id}.{file_id}.{ext}` to `{table_name}/{load_id}.{file_id}.{ext}` in dlt 0.3.12. You can revert to the old layout by setting it manually.
:::

### Available layout placeholders

#### Standard placeholders

* `schema_name` - the name of the [schema](../../general-usage/schema.md)
* `table_name` - the table name
* `load_id` - the ID of the [load package](../../general-usage/destination-tables.md#load-packages-and-load-ids) from which the file comes
* `file_id` - the ID of the file; if there are many files with data for a single table, they are copied with different file IDs
* `ext` - the format of the file, i.e., `jsonl` or `parquet`

#### Date and time placeholders
:::tip
Keep in mind all values are lowercased.
:::

* `timestamp` - the current timestamp in Unix Timestamp format rounded to seconds
* `timestamp_ms` - the current timestamp in Unix Timestamp format in milliseconds
* `load_package_timestamp` - timestamp from [load package](../../general-usage/destination-tables.md#load-packages-and-load-ids) in Unix Timestamp format rounded to seconds
* `load_package_timestamp_ms` - timestamp from [load package](../../general-usage/destination-tables.md#load-packages-and-load-ids) in Unix Timestamp format in milliseconds

:::note
Both `timestamp_ms` and `load_package_timestamp_ms` are in milliseconds (e.g., 12334455233), not fractional seconds to ensure millisecond precision without decimals.
:::

* Years
  * `YYYY` - 2024, 2025
  * `Y` - 2024, 2025
* Months
  * `MMMM` - January, February, March
  * `MMM` - Jan, Feb, Mar
  * `MM` - 01, 02, 03
  * `M` - 1, 2, 3
* Days of the month
  * `DD` - 01, 02
  * `D` - 1, 2
* Hours 24h format
  * `HH` - 00, 01, 02...23
  * `H` - 0, 1, 2...23
* Minutes
  * `mm` - 00, 01, 02...59
  * `m` - 0, 1, 2...59
* Seconds
  * `ss` - 00, 01, 02...59
  * `s` - 0, 1, 2...59
* Fractional seconds
  * `SSSS` - 000[0..] 001[0..] ... 998[0..] 999[0..]
  * `SSS` - 000 001 ... 998 999
  * `SS` - 00, 01, 02 ... 98, 99
  * `S` - 0 1 ... 8 9
* Days of the week
  * `dddd` - Monday, Tuesday, Wednesday
  * `ddd` - Mon, Tue, Wed
  * `dd` - Mo, Tu, We
  * `d` - 0-6
* `Q` - quarters 1, 2, 3, 4

You can change the file name format by providing the layout setting for the filesystem destination like so:
```toml
[destination.filesystem]
layout="{table_name}/{load_id}.{file_id}.{ext}" # current preconfigured naming scheme

# More examples
# With timestamp
# layout = "{table_name}/{timestamp}/{load_id}.{file_id}.{ext}"

# With timestamp of the load package
# layout = "{table_name}/{load_package_timestamp}/{load_id}.{file_id}.{ext}"

# Parquet-like layout (note: it is not compatible with the internal datetime of the parquet file)
# layout = "{table_name}/year={year}/month={month}/day={day}/{load_id}.{file_id}.{ext}"

# Custom placeholders
# extra_placeholders = { "owner" = "admin", "department" = "finance" }
# layout = "{table_name}/{owner}/{department}/{load_id}.{file_id}.{ext}"
```

A few things to know when specifying your filename layout:
- If you want a different base path that is common to all filenames, you can suffix your `bucket_url` rather than prefix your `layout` setting.
- If you do not provide the `{ext}` placeholder, it will automatically be added to your layout at the end with a dot as a separator.
- It is best practice to have a separator between each placeholder. Separators can be any character allowed as a filename character, but dots, dashes, and forward slashes are most common.
- When you are using the `replace` disposition, `dlt` will have to be able to figure out the correct files to delete before loading the new data. For this to work, you have to:
  - include the `{table_name}` placeholder in your layout
  - not have any other placeholders except for the `{schema_name}` placeholder before the table_name placeholder and
  - have a separator after the table_name placeholder

Please note:
- `dlt` will mark complete loads by creating a json file in the `./_dlt_loads` folders that corresponds to the `_dlt_loads` table. For example, if the `chess__1685299832.jsonl` file is present in the loads folder, you can be sure that all files for the load package `1685299832` are completely loaded.

### Advanced layout configuration

The filesystem destination configuration supports advanced layout customization and the inclusion of additional placeholders. This can be done through `config.toml` or programmatically when initializing via a factory method.

#### Configuration via `config.toml`

To configure the layout and placeholders using `config.toml`, use the following format:

```toml
[destination.filesystem]
layout = "{table_name}/{test_placeholder}/{YYYY}-{MM}-{DD}/{ddd}/{mm}/{load_id}.{file_id}.{ext}"
extra_placeholders = { "test_placeholder" = "test_value" }
current_datetime="2024-04-14T00:00:00"
# for automatic directory creation in the local filesystem
kwargs = '{"auto_mkdir": true}'
```

:::note
Ensure that the placeholder names match the intended usage. For example, `{test_placeholer}` should be corrected to `{test_placeholder}` for consistency.
:::

#### Dynamic configuration in the code

Configuration options, including layout and placeholders, can be overridden dynamically when initializing and passing the filesystem destination directly to the pipeline.

```py
import pendulum

import dlt
from dlt.destinations import filesystem

pipeline = dlt.pipeline(
    pipeline_name="data_things",
    destination=filesystem(
        layout="{table_name}/{test_placeholder}/{timestamp}/{load_id}.{file_id}.{ext}",
        current_datetime=pendulum.now(),
        extra_placeholders={
            "test_placeholder": "test_value",
        }
    )
)
```

Furthermore, it is possible to:

1. Customize the behavior with callbacks for extra placeholder functionality. Each callback must accept the following positional arguments and return a string.
2. Customize the `current_datetime`, which can also be a callback function and is expected to return a `pendulum.DateTime` instance.

```py
import pendulum

import dlt
from dlt.destinations import filesystem

def placeholder_callback(schema_name: str, table_name: str, load_id: str, file_id: str, ext: str) -> str:
    # Custom logic here
    return "custom_value"

def get_current_datetime() -> pendulum.DateTime:
    return pendulum.now()

pipeline = dlt.pipeline(
    pipeline_name="data_things",
    destination=filesystem(
        layout="{table_name}/{placeholder_x}/{timestamp}/{load_id}.{file_id}.{ext}",
        current_datetime=get_current_datetime,
        extra_placeholders={
            "placeholder_x": placeholder_callback
        }
    )
)
```

### Recommended layout

The currently recommended layout structure is straightforward:

```toml
layout="{table_name}/{load_id}.{file_id}.{ext}"
```

Adopting this layout offers several advantages:
1. **Efficiency:** It's fast and simple to process.
2. **Compatibility:** Supports `replace` as the write disposition method.
3. **Flexibility:** Compatible with various destinations, including Athena.
4. **Performance:** A deeply nested structure can slow down file navigation, whereas a simpler layout mitigates this issue.

## Supported file formats

You can choose the following file formats:
* [jsonl](../file-formats/jsonl.md) is used by default
* [parquet](../file-formats/parquet.md) is supported
* [csv](../file-formats/csv.md) is supported

## Supported table formats

You can choose the following table formats:
* [Delta](../table-formats/delta.md) is supported

### Delta table format

You need the `deltalake` package to use this format:

```sh
pip install "dlt[deltalake]"
```

You also need `pyarrow>=17.0.0`:

```sh
pip install 'pyarrow>=17.0.0'
```

Set the `table_format` argument to `delta` when defining your resource:

```py
@dlt.resource(table_format="delta")
def my_delta_resource():
    ...
```

> `dlt` always uses `parquet` as `loader_file_format` when using the `delta` table format. Any setting of `loader_file_format` is disregarded.

#### Delta table partitioning
A Delta table can be partitioned ([Hive-style partitioning](https://delta.io/blog/pros-cons-hive-style-partionining/)) by specifying one or more `partition` column hints. This example partitions the Delta table by the `foo` column:

```py
@dlt.resource(
  table_format="delta",
  columns={"foo": {"partition": True}}
)
def my_delta_resource():
    ...
```

:::caution
It is **not** possible to change partition columns after the Delta table has been created. Trying to do so causes an error stating that the partition columns don't match.
:::


#### Storage options
You can pass storage options by configuring `destination.filesystem.deltalake_storage_options`:

```toml
[destination.filesystem]
deltalake_storage_options = '{"AWS_S3_LOCKING_PROVIDER": "dynamodb", "DELTA_DYNAMO_TABLE_NAME": "custom_table_name"}'
```

`dlt` passes these options to the `storage_options` argument of the `write_deltalake` method in the `deltalake` library. Look at their [documentation](https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.write_deltalake) to see which options can be used.

You don't need to specify credentials here. `dlt` merges the required credentials with the options you provided before passing it as `storage_options`.

>❗When using `s3`, you need to specify storage options to [configure](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/) locking behavior.

#### `get_delta_tables` helper
You can use the `get_delta_tables` helper function to get `deltalake` [DeltaTable](https://delta-io.github.io/delta-rs/api/delta_table/) objects for your Delta tables:

```py
from dlt.common.libs.deltalake import get_delta_tables

...

# get dictionary of DeltaTable objects
delta_tables = get_delta_tables(pipeline)

# execute operations on DeltaTable objects
delta_tables["my_delta_table"].optimize.compact()
delta_tables["another_delta_table"].optimize.z_order(["col_a", "col_b"])
# delta_tables["my_delta_table"].vacuum()
# etc.

```

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination). To this end, special folders and files will be created at your destination which hold information about your pipeline state, schemas, and completed loads. These folders DO NOT respect your settings in the layout section. When using filesystem as a staging destination, not all of these folders are created, as the state and schemas are managed in the regular way by the final destination you have configured.

You will also notice `init` files being present in the root folder and the special `dlt` folders. In the absence of the concepts of schemas and tables in blob storages and directories, `dlt` uses these special files to harmonize the behavior of the `filesystem` destination with the other implemented destinations.

:::note
When a load generates a new state, for example when using incremental loads, a new state file appears in the `_dlt_pipeline_state` folder at the destination. To prevent data accumulation, state cleanup mechanisms automatically remove old state files, retaining only the latest 100 by default. This cleanup process can be customized or disabled using the filesystem configuration `max_state_files`, which determines the maximum number of pipeline state files to retain (default is 100). Setting this value to 0 or a negative number disables the cleanup of old states.
:::

## Troubleshooting
### File Name Too Long Error
When running your pipeline, you might encounter an error like `[Errno 36] File name too long Error`. This error occurs because the generated file name exceeds the maximum allowed length on your filesystem.

To prevent the file name length error, set the `max_identifier_length` parameter for your destination. This truncates all identifiers (including filenames) to a specified maximum length.
For example: 

```py
from dlt.destinations import duckdb

pipeline = dlt.pipeline(
    pipeline_name="your_pipeline_name",
    destination=duckdb(
        max_identifier_length=200,  # Adjust the length as needed
    ),
)
```

:::note
- `max_identifier_length` truncates all identifiers (tables, columns). Ensure the length maintains uniqueness to avoid collisions.
- Adjust `max_identifier_length` based on your data structure and filesystem limits.
:::

<!--@@@DLT_TUBA filesystem-->
