FROM ubuntu:noble

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y openssh-server && \
    rm -rf /var/lib/apt/lists/*

# Certificate Authority (CA): public key
COPY ca_rsa.pub /etc/ssh/ca_rsa.pub
RUN chmod 600 /etc/ssh/ca_rsa.pub

RUN mkdir -p /etc/ssh/auth_principals && \
    echo "billy" >> /etc/ssh/auth_principals/billy

RUN mkdir -p /run/sshd && \
    echo "SyslogFacility AUTH" >> /etc/ssh/sshd_config && \
    echo "LogLevel INFO" >> /etc/ssh/sshd_config && \
    echo "TrustedUserCAKeys /etc/ssh/ca_rsa.pub" >> /etc/ssh/sshd_config && \
    echo "AuthorizedPrincipalsFile /etc/ssh/auth_principals/billy" >> /etc/ssh/sshd_config

# USER foo: set up user for SFTP with no shell login
RUN useradd -m -d /home/foo -s /usr/sbin/nologin foo && \
    mkdir -p /home/foo/.ssh && \
    chown foo:foo /home/foo/.ssh && \
    chmod 700 /home/foo/.ssh

# USER foo: set password for the user foo
RUN echo 'foo:pass' | chpasswd

# USER foo: copy the public key and set permissions
COPY foo_rsa.pub /home/foo/.ssh/authorized_keys
RUN chmod 600 /home/foo/.ssh/authorized_keys && \
    chown foo:foo /home/foo/.ssh/authorized_keys

# USER foo: create a directory for SFTP that the user will have access to
RUN mkdir -p /home/foo/sftp/data && \
    chown root:root /home/foo /home/foo/sftp && \
    chmod 755 /home/foo /home/foo/sftp && \
    chown foo:foo /home/foo/sftp/data && \
    chmod 755 /home/foo/sftp/data


# USER foo: configure SSH for SFTP: allow password and pubkey authentication
RUN echo "Match User foo" >> /etc/ssh/sshd_config && \
    echo "    ChrootDirectory /home/foo/sftp" >> /etc/ssh/sshd_config && \
    echo "    ForceCommand internal-sftp" >> /etc/ssh/sshd_config && \
    echo "    PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PermitTunnel no" >> /etc/ssh/sshd_config && \
    echo "    AllowAgentForwarding no" >> /etc/ssh/sshd_config && \
    echo "    AllowTcpForwarding no" >> /etc/ssh/sshd_config && \
    echo "    X11Forwarding no" >> /etc/ssh/sshd_config

# USER bobby: set up user for SFTP with no shell login
RUN useradd -m -d /home/bobby -s /usr/sbin/nologin bobby && \
    mkdir -p /home/bobby/.ssh && \
    chown bobby:bobby /home/bobby/.ssh && \
    chmod 700 /home/bobby/.ssh

# USER bobby: copy the public key and set permissions
COPY bobby_rsa.pub /home/bobby/.ssh/authorized_keys
RUN chmod 600 /home/bobby/.ssh/authorized_keys && \
    chown bobby:bobby /home/bobby/.ssh/authorized_keys

# USER bobby: create a directory for SFTP that the user will have access to
RUN mkdir -p /home/bobby/sftp/data && \
    chown root:root /home/bobby /home/bobby/sftp && \
    chmod 755 /home/bobby /home/bobby/sftp && \
    chown bobby:bobby /home/bobby/sftp/data && \
    chmod 755 /home/bobby/sftp/data

# USER bobby: configure SSH for SFTP: allow password and pubkey authentication
RUN echo "Match User bobby" >> /etc/ssh/sshd_config && \
    echo "    ChrootDirectory /home/bobby/sftp" >> /etc/ssh/sshd_config && \
    echo "    ForceCommand internal-sftp" >> /etc/ssh/sshd_config && \
    echo "    PasswordAuthentication no" >> /etc/ssh/sshd_config && \
    echo "    PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PermitTunnel no" >> /etc/ssh/sshd_config && \
    echo "    AllowAgentForwarding no" >> /etc/ssh/sshd_config && \
    echo "    AllowTcpForwarding no" >> /etc/ssh/sshd_config && \
    echo "    X11Forwarding no" >> /etc/ssh/sshd_config   

# USER billy: set up user for SFTP with no shell login
RUN useradd -m -d /home/billy -s /usr/sbin/nologin billy && \
    mkdir -p /home/billy/.ssh && \
    chown billy:billy /home/billy/.ssh && \
    chmod 700 /home/billy/.ssh

# USER billy: create a directory for SFTP that the user will have access to
RUN mkdir -p /home/billy/sftp/data && \
    chown root:root /home/billy /home/billy/sftp && \
    chmod 755 /home/billy /home/billy/sftp && \
    chown billy:billy /home/billy/sftp/data && \
    chmod 755 /home/billy/sftp/data

# USER billy: certificated signed with CA key
COPY billy_rsa-cert.pub /home/billy/.ssh/billy_rsa-cert.pub

RUN chown billy:billy /home/billy/.ssh/billy_rsa-cert.pub && \
    chmod 600 /home/billy/.ssh/billy_rsa-cert.pub

# USER billy: configure SSH for SFTP with certificate authentication
RUN echo "Match User billy" >> /etc/ssh/sshd_config && \
    echo "    ChrootDirectory /home/billy/sftp" >> /etc/ssh/sshd_config && \
    echo "    ForceCommand internal-sftp" >> /etc/ssh/sshd_config && \
    echo "    PasswordAuthentication no" >> /etc/ssh/sshd_config && \
    echo "    PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "    PermitTunnel no" >> /etc/ssh/sshd_config && \
    echo "    AllowAgentForwarding no" >> /etc/ssh/sshd_config && \
    echo "    AllowTcpForwarding no" >> /etc/ssh/sshd_config && \
    echo "    X11Forwarding no" >> /etc/ssh/sshd_config

EXPOSE 22

# run sshd on container start
CMD ["/usr/sbin/sshd", "-D", "-e"]
