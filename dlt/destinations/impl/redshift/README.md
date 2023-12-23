# Public Access setup
There's *Modify publicly accessible settings* in Actions of each Redshift cluster. Assign your IP there.

# Runtime optimization

https://www.intermix.io/blog/top-14-performance-tuning-techniques-for-amazon-redshift/

1. we should use separate work queue for loader user
2. they suggest to not use dist keys
3. data must be inserted in order of sortkey

# loader account setup

1. Create new database `CREATE DATABASE dlt_ci`
2. Create new user, set password
3. Set as database owner (we could set lower permission) `ALTER DATABASE dlt_ci OWNER TO loader`

# Public access setup for Serverless
Follow https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-connecting.html `Connecting from the public subnet to the Amazon Redshift Serverless endpoint using Network Load Balancer`

that will use terraform template to create load balancer endpoint and assign public IP. The cost of the load balancer is ~16$/month + cost of IP

It seems that port 5439 is closed to the VPC on which serverless redshift created itself. In the cluster panel: Data Access : VPC security group add Inbound Rule to allow 5439 port from any subnet 0.0.0.0/0