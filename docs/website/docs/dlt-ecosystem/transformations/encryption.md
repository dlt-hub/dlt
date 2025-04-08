---
title: Data security and encryption
description: Transforming the data for encryption in transit, client-side encryption, and encryption at rest.
keywords: [transform, data security, encryption]
---

# Data security and encryption

The modern data landscape requires robust security measures to protect sensitive information throughout its lifecycle. Organizations must implement comprehensive encryption strategies to safeguard data against increasingly sophisticated threats

### Encryption throughout the lifecycle

Data is vulnerable both while moving between systems (in transit) and when stored at rest. dlt addresses both needs:

- **Data in transit**:

    
    During transfers, data can be intercepted or tampered with. Employing secure protocols (e.g., SSL/TLS) or encrypted connection strings prevents unauthorized access and ensures confidentiality.
    
- **Client-side encryption**:

    
    Encrypting data on the client side before transmission adds an extra layer of security. By using libraries like AWS Encryption SDK, Google Tink, or Azure Key Vault, organizations can ensure that data is encrypted before it leaves the source, maintaining control over encryption keys and safeguarding sensitive information throughout its journey.
    
- **Data at rest / Server-side encryption**:

    
    Even if a storage system is compromised, encryption at rest prevents unauthorized access. dlt can leverage disk encryption (e.g., BitLocker, FileVault, dm-crypt/LUKS) or the destination’s server-side encryption to keep data safe on the local machine and in the cloud.
    

This layered approach ensures comprehensive protection, covering every stage of data’s journey from extraction to storage.

## Client-side encryption

### Why client-side encryption?

Client-side encryption empowers you to encrypt data before it ever leaves your environment. This ensures that:

- **You maintain full control of encryption keys** (often managed through KMS solutions like AWS KMS, Google Cloud KMS, or Azure Key Vault).
- **Data remains protected** during transit and while stored at the destination, even if the destination’s security is compromised.

### Common client-side encryption tools

- AWS Encryption SDK
- Tink by Google
- Azure Key Vault & Blob Client-Side Encryption
- OpenSSL


## Encryption in transit and server-side encryption

### Encrypted partitions for dlt’s working directory

Because dlt extracts and processes data locally before loading it to a destination, the files on your local disk contain sensitive data. Encrypting the partition where these files reside ensures your data is protected at rest on the local system. You can use the following systems to safeguard your data at rest.

- **Windows**: BitLocker
- **macOS**: FileVault
- **Linux**: dm-crypt/LUKS, Loop-AES

### Encryption in transit

Encrypting data while it travels to the destination is equally important. With dlt, you can configure your connection string to enforce encryption. Depending on the target database or storage, options might include:

- Adding `Encrypt=yes` and `encrypt=true` to your **connection string** to ensure TLS/SSL is used for all communication.
- Using secure protocols such as HTTPS for cloud-based destinations.

### Server-side encryption

Server-side encryption (SSE) complements your client-side encryption measures by encrypting data once it arrives at the destination. Popular services include:

- **Amazon S3 SSE**: Offers SSE-S3 (using AWS-managed keys) or SSE-KMS (using customer-managed keys in AWS KMS).
- **BigQuery**: Provides options for customer-managed encryption keys (CMEK), allowing for more granular key management.

## Managing encryption in dlt

### Steps for setting up encrypted pipelines

1. **Provision encryption keys**: Use your preferred cloud KMS or on-prem solution to create and manage cryptographic keys.
2. **Integrate with Client-Side Libraries**: Install and configure the relevant encryption SDK (e.g., AWS Encryption SDK) in your environment.
3. **Incorporate encryption logic**: Modify your dlt resource functions to encrypt sensitive fields before they are loaded to the destination.
4. **Test and validate**: Ensure that your data is encrypted and can be decrypted correctly using your keys.

Below is an example showing how to encrypt specific fields in a nested data structure using AWS KMS before loading data with dlt. The same principle can be applied to other encryption libraries, such as Google Tink or Azure Key Vault.

:::note
For demonstration purposes, we have included "KMS_KEY_ARN", "aws_access_key_id", and "aws_secret_access_key" in the script. However, it is best practice to store these in dlt `secrets.toml` or a secure vault.
:::

```py
import boto3
import dlt
import aws_encryption_sdk
from aws_encryption_sdk import CommitmentPolicy
from aws_encryption_sdk.key_providers.kms import KMSMasterKey

# Define the KMS Key ARN
KMS_KEY_ARN = (
    "arn:aws:kms:<region>:<number>:key/<key>"
)

# Create a boto3 client for AWS KMS
kms_client = boto3.client(
    "kms",
    region_name="<region-name>",
    aws_access_key_id="your aws access key",
    aws_secret_access_key="your aws secret key",
)

# Create the KMS Master Key
master_key = KMSMasterKey(key_id=KMS_KEY_ARN, client=kms_client)

# Instantiate the AWS Encryption SDK client
client = aws_encryption_sdk.EncryptionSDKClient(
    commitment_policy=CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
)

#Encryption function
def encryption_func(record):
    """
    Encrypts the 'security_key' for each child in a given record.
    """
    for child in record.get("children", []):
        # Convert the security key to bytes for encryption
        key_to_encrypt = str(child["security_key"]).encode("utf-8")
        try:
            # Encrypt the security key using the provided master key
            ciphertext, _ = client.encrypt(
                source=key_to_encrypt, key_provider=master_key
            )
            # Replace the plain key with the encrypted data in hex format
            child["security_key"] = ciphertext.hex()
        except Exception as e:
            print(
                f"Failed to encrypt security key for child_id {child['child_id']}: {e}"
            )
            raise
    return record


# Define the raw data structure
raw_data = [
    {
        "parent_id": 1,
        "parent_name": "Alice",
        "children": [
            {"child_id": 1, "child_name": "Child 1", "security_key": 12345},
            {"child_id": 2, "child_name": "Child 2", "security_key": 67891},
        ],
    },
    {
        "parent_id": 2,
        "parent_name": "Bob",
        "children": [
            {"child_id": 3, "child_name": "Child 3", "security_key": 999111}
        ],
    },
]

#dlt resource
@dlt.resource(name="data_test", write_disposition={"disposition": "replace"})
def data_source():
    yield from raw_data


if __name__ == "__main__":
    # Apply the encryption transformation using add_map
    data_encrypted = data_source().add_map(encryption_func)

    # Configure and run the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="pipeline",
        destination="duckdb",
        dataset_name="dataset",
    )
    load_info = pipeline.run(data_encrypted)
    print(load_info)
```

In this code:

- The AWS Encryption SDK is used to encrypt the `security_key` field before loading the data with dlt.
- You can adapt this pattern to encrypt other sensitive fields or integrate different encryption libraries.
- Various encryption methods can be employed for client-side encryption.

## Security best practices

**1. Combine client-side and server-side encryption**

For maximum security, encrypt data on the client side and also enable server-side encryption at the destination. This ensures data remains secure, even if one layer of security fails or is misconfigured.

**2. Key management and rotation**

Use a dedicated Key Management Service (KMS) such as AWS KMS, Google Cloud KMS, or Azure Key Vault to store and manage your encryption keys. Rotate keys regularly and enforce strict access controls.

**3. Secure your infrastructure**

Encrypt the local disk or partition where dlt extracts and processes data to ensure the data at rest on your system is protected (e.g., BitLocker, FileVault, dm-crypt/LUKS).

**4. Monitor and audit**

Implement monitoring for unusual access patterns, and maintain detailed logs for auditing. Services like AWS CloudTrail or Azure Monitor can provide insights into who accessed your keys and when.

**5. Validate and test**

Regularly test your encryption and decryption workflows in a staging or QA environment. Confirm that you can restore data from backups and that your encryption processes don’t introduce bottlenecks or errors.