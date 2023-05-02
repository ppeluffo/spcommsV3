#!/bin/bash 
/etc/init.d/rsyslog start
/etc/init.d/apache2 restart
/usr/sbin/logrotate /etc/logrotate.conf & 
tail -f /var/log/apache2/access.log & tail -f /var/log/apache2/error.log & tail -f /var/log/spcomms.log
