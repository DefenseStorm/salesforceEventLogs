Salesforce Integration for DefenseStorm

to pull this repository and submodules into /usr/local:

As root:

    cd /usr/local

    git clone --recurse-submodules https://github.com/DefenseStorm/salesforceEventLogs.git

1. If this is the first integration on this DVM, Do the following:

  cp ds-integration/ds_events.conf to /etc/syslog-ng/conf.d

  Edit /etc/syslog-ng/syslog-ng.conf and add local7 to the excluded list for filter f_syslog3 and filter f_messages.  The lines should look like the following:

  filter f_syslog3 { not facility(auth, authpriv, mail, local7) and not filter(f_debug); };
  
  filter f_messages { level(info,notice,warn) and
                    not facility(auth,authpriv,cron,daemon,mail,news,local7); };


  Restart syslog-ng
    service syslog-ng restart

2. Copy the template config file and update the settings

  cp salesforceEventLogs.conf.template salesforceEventLogs.conf

  change the following items in the config file based on your configuration
      username
      password
      security_token
      instance_url

3. DAILY RUNS: Set "interval" in conf file to "daily" and add the following entry to the root crontab so the script will run every day at 2am

   0 2 * * * cd /usr/local/salesforceEventLogs; ./salesforceEventLogs.py

   DAILY RUNS: Set "interval" in conf file to "hourly" and add the following entry to the root crontab so the script will run every hour

   0 * * * * cd /usr/local/salesforceEventLogs; ./salesforceEventLogs.py

Testing:

- To Execute by Hand:
    cd /usr/local/salesforceEventLogs
    ./salesforceEventLogs

- Local logs are in /var/log/syslog.  To watch real time:
    tail -f /var/log/syslog
