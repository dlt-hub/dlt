FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

# update packages and install openssh server
RUN apt-get update && \
    apt-get install -y openssh-server && \
    rm -rf /var/lib/apt/lists/*

# set up user for SFTP with no shell login
RUN useradd -m -d /home/foo -s /usr/sbin/nologin foo && \
    mkdir -p /home/foo/.ssh && \
    chown foo:foo /home/foo/.ssh && \
    chmod 700 /home/foo/.ssh

# set password for the user foo
RUN echo 'foo:pass' | chpasswd

# copy the public key and set permissions
COPY foo_rsa.pub /home/foo/.ssh/authorized_keys
RUN chmod 600 /home/foo/.ssh/authorized_keys && \
    chown foo:foo /home/foo/.ssh/authorized_keys

# create a directory for SFTP that the user will have access to
RUN mkdir -p /home/foo/sftp/data && \
    chown root:root /home/foo /home/foo/sftp && \
    chmod 755 /home/foo /home/foo/sftp && \
    chown foo:foo /home/foo/sftp/data && \
    chmod 755 /home/foo/sftp/data

# configure SSH for SFTP: allow password and pubkey authentication
RUN mkdir -p /run/sshd && \
    echo "Match User foo" >> /etc/ssh/sshd_config && \
    echo "    ChrootDirectory /home/foo/sftp" >> /etc/ssh/sshd_config && \
    echo "    ForceCommand internal-sftp" >> /etc/ssh/sshd_config && \
    echo "    PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PermitTunnel no" >> /etc/ssh/sshd_config && \
    echo "    AllowAgentForwarding no" >> /etc/ssh/sshd_config && \
    echo "    AllowTcpForwarding no" >> /etc/ssh/sshd_config && \
    echo "    X11Forwarding no" >> /etc/ssh/sshd_config

EXPOSE 22

# run sshd on container start
CMD ["/usr/sbin/sshd", "-D", "-e"]
