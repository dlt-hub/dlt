from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    GcpOAuthCredentials,
    GcpCredentials,
    AwsCredentials,
    AzureCredentials,
    AzureServicePrincipalCredentials,
    SFTPCredentials,
)
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.specs import OAuth2Credentials
from dlt.common.configuration.specs import CredentialsConfiguration, configspec
from dlt.common.storages.configuration import (
    FileSystemCredentials,
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
)


__all__ = [
    "GcpServiceAccountCredentials",
    "GcpOAuthCredentials",
    "GcpCredentials",
    "AwsCredentials",
    "AzureCredentials",
    "ConnectionStringCredentials",
    "OAuth2Credentials",
    "CredentialsConfiguration",
    "configspec",
    "FileSystemCredentials",
    "FilesystemConfiguration",
    "FilesystemConfigurationWithLocalFiles",
    "AzureServicePrincipalCredentials",
    "SFTPCredentials",
]
