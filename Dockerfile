FROM ubuntu:20.04

ENV TZ=America/Montevideo
RUN echo $TZ > /etc/timezone && \
apt-get update && apt-get install -y tzdata && \
rm /etc/localtime && \
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
dpkg-reconfigure -f noninteractive tzdata && \
apt-get clean

RUN \
    DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -y \
        build-essential \
        apt-utils \
        ssl-cert \
        apache2 \
        apache2-utils \
        apache2-dev \
        libapache2-mod-perl2 \
        libapache2-mod-perl2-dev \
        libcgi-pm-perl \
        liblocal-lib-perl \
        libexpat1-dev \
        libssl-dev \
        libapreq2-dev \
        zip && \
    a2enmod cgid && \
    a2enmod rewrite && \
    a2dissite 000-default && \
    apt-get update -y && \
    apt-get upgrade -y && \
    apt-get -y clean

RUN \   
    apt install libdatetime-perl -y && \
    apt install libxml-simple-perl -y && \
    apt install libpq-dev build-essential -y

RUN apt install python3.8 python3-pip -y
COPY ./spcommsV3/Requirements.txt /tmp/Requirements.txt
RUN pip3 install -r /tmp/Requirements.txt
RUN apt install -y rsyslog logrotate
RUN sed -i '/imklog/s/^/#/' /etc/rsyslog.conf 
RUN apt-get autoclean -y


RUN echo ':syslogtag, contains, "SPCOMMSV3" /var/log/spcomms.log \n \
:syslogtag, contains, "SPCOMMSV3" ~ \n' >> /etc/rsyslog.d/50-default.conf

COPY ./apache2/001-sitio.conf /etc/apache2/sites-available/001-sitio.conf
RUN a2ensite 001-sitio

RUN mkdir -p /datos/cgi-bin

COPY spcommsV3 /datos/cgi-bin

RUN chmod -R 777 /datos

RUN touch /var/log/spcomms.log && \
    chmod 777 /var/log/spcomms.log

COPY server_entrypoint.sh /server_entrypoint.sh
RUN chmod +x /server_entrypoint.sh

CMD /server_entrypoint.sh

EXPOSE 80