## Users: Authentication

This guide covers the setup of different authentication methods for SSH users, including public/private key pairs, passphrase protection, and certificate-based authentication.

### User foo: Public/Private Key Pair Without Passphrase

Generate a key pair for `foo` without a passphrase:
```bash
# Generate the key pair
ssh-keygen -t rsa -b 4096 -C "foo@example.com" -f foo_rsa

# Secure the private key
chmod 600 foo_rsa
```

### User bobby: Public/Private Key Pair With Passphrase

Generate a key pair for `bobby` with a passphrase (passphrase=passphrase123):
```bash
# Generate the key pair with a passphrase
ssh-keygen -t rsa -b 4096 -C "bobby@example.com" -f bobby_rsa

# Secure the private key
chmod 600 bobby_rsa
```

### Certificate Authority (CA) Setup

Generate the Certificate Authority (CA) key pair:
```bash
# Generate a self-signed CA key pair
ssh-keygen -t rsa -b 4096 -f ca_rsa -N ""
```

### User billy: Public/Private Key Pair with CA-Signed Certificate

Generate and sign a key pair for `billy` using the CA:
```bash
# Generate the user key pair for billy
ssh-keygen -t rsa -b 4096 -C "billy@example.com" -f billy_rsa

# Sign billy's public key with the CA
ssh-keygen -s ca_rsa -I billy-cert -n billy billy_rsa.pub
```

### Important Files

- **ca_rsa.pub**: The CA public key. This key is used by the server to verify certificates.
- **billy_rsa-cert.pub**: Billy’s signed certificate. This certificate is used by Billy to authenticate with the server.
