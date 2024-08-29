import fsspec

# fsspec ssh_args available are:
# - hostname, port=22, username=None, password=None, pkey=None, key_filename=None, timeout=None, allow_agent=True, look_for_keys=True, compress=False, sock=None, gss_auth=False, gss_kex=False, gss_deleg_creds=True, gss_host=None, banner_timeout=None, auth_timeout=None, channel_timeout=None, gss_trust_dns=True, passphrase=None, disabled_algorithms=None, transport_factory=None, auth_strategy=None
# - url: https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect

# Set up connection to the localhost SFTP server:
## 1. using generated ssh key
fs = fsspec.filesystem("sftp", host="localhost", port=2222, username="foo", key_filename="foo_rsa")

## 2. using linux user and password
# fs = fsspec.filesystem("sftp", host="localhost", port=2222, username="foo", password = "pass")

# List files on the SFTP server
print(fs.ls("/data"))

# Write data to a file
with fs.open("/data/hello.txt", "w") as f:
    f.write("This is a new file added via SFTP!")

# Read data from the file
with fs.open("/data/hello.txt", "r") as f:
    data = f.read()
    print(data)

# List files on the SFTP server
print(fs.ls("/data"))
