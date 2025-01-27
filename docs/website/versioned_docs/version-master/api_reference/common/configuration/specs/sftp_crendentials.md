---
sidebar_label: sftp_crendentials
title: common.configuration.specs.sftp_crendentials
---

## SFTPCredentials Objects

```python
@configspec
class SFTPCredentials(CredentialsConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/sftp_crendentials.py#L8)

Credentials for SFTP filesystem, compatible with fsspec SFTP protocol.

Authentication is attempted in the following order of priority:

    - `key_filename` may contain OpenSSH public certificate paths
      as well as regular private-key paths; when files ending in `-cert.pub` are found, they are assumed to match
      a private key, and both components will be loaded.

    - Any key found through an SSH agent: any “id_rsa”, “id_dsa”, or “id_ecdsa” key discoverable in ~/.ssh/.

    - Plain username/password authentication, if a password was provided.

    - If a private key requires a password to unlock it, and a password is provided, that password will be used to
      attempt to unlock the key.

For more information about parameters:
https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect

### to\_fsspec\_credentials

```python
def to_fsspec_credentials() -> Dict[str, Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/configuration/specs/sftp_crendentials.py#L46)

Return a dict that can be passed to fsspec SFTP/SSHClient.connect method.

