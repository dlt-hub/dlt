# AWS S3 filesystem

## Initialize the verified source

1. To load the data to the AWS S3 file system, you must first initialise the project as follows:
   ```bash
   dlt init chess filesystem
   ```
   > This command will initialise your pipeline with chess as the source and the AWS S3 filesystem as the destination.

2. Install the necessary dependencies for the S3 filesystem by running:

   ```
   pip install -r requirements.txt
   ```

## Grab credentials

To grab the required credentials you need to create a S3 bucket and a user who can access that bucket using certain permissions/policies.

1. You can create the S3 bucket in AWS console by clicking on "Create Bucket" in S3 and assigning appropriate name and permissions to the bucket.
2. Once the bucket is created you'll have the bucket URL. For example, If the bucket name is "mybucket_filesystem", then the bucket URL will be:

   ```
   s3://mybucket_filesystem
   ```

3. To grant permissions to the user being used to access the S3 bucket, go to the IAM > Users, and click on “Add Permissions”.
4. To add the required roles to the policy please refer to [this documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket-console.html), or you can generate   your custom policy using [AWS policy generator](https://awspolicygen.s3.amazonaws.com/policygen.html). The roles required to read/write to the S3 bucket are :
    
| Role | Description/Actions |
| --- | --- |
| s3:ListBucket | permission is necessary to retrieve a list of existing buckets. It allows you to view available buckets and select the bucket where you want to write the data |
| s3:*Object | GetObject, DeleteObject, PutObject, and any other Amazon S3 action that ends with the word "Object"  |
| s3ConsoleAccess | GetAccountPublicAccessBlock, GetBucketAcl, GetBucketLocation, GetBucketPolicyStatus, ListAccessPoints, ListAllMyBuckets |
6. To grab the access and secret key for the user. Go to IAM > Users and in the “Security Credentials”, click on “Create Access Key”, and preferably select “Command Line Interface” and create the access key.
7. Grab the “Access Key” and “Secret Access Key” created that are to be used in "secrets.toml".

## Configure `secrets.toml`

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`, which looks like this:

  ```
  [destination.filesystem]
  bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

  [destination.filesystem.credentials]
  aws_access_key_id = "please set me up!" # copy the access key here
  aws_secret_access_key = "please set me up!" # copy the secret access key here
  ```
  Please, enter the credentials [grabbed](filesystem.md#grab-credentials) in the file above. 

That’s it! This is how you can set up your AWS S3 filesystem credentials.

        
