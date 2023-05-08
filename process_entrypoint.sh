#!/bin/bash 
/etc/init.d/rsyslog start
/usr/sbin/logrotate /etc/logrotate.conf & 
python3 spcomms_process_multiprocess.py & 
tail -f /var/log/spcomms.log
