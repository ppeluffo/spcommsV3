#!/bin/bash 
/etc/init.d/rsyslog start
/usr/sbin/logrotate /etc/logrotate.conf & 
tail -f /var/log/spcomms.log
