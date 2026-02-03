#!/bin/bash

echo "Running on the driver? $DB_IS_DRIVER"
echo "Driver ip: $DB_DRIVER_IP"

cat <<EOF >> /tmp/start_datadog.sh
#!/bin/bash

if [ \$DB_IS_DRIVER ]; then
  echo "On the driver. Installing Datadog ..."

  # install the Datadog agent
  DD_API_KEY=<API KEY> bash -c "\$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)"

  # WAITING UNTIL MASTER PARAMS ARE LOADED, THEN GRABBING IP AND PORT
  while [ -z \$gotparams ]; do
    if [ -e "/tmp/master-params" ]; then
      DB_DRIVER_PORT=\$(cat /tmp/master-params | cut -d' ' -f2)
      gotparams=TRUE
    fi
    sleep 2
  done

  current=\$(hostname -I | xargs)

  # WRITING SPARK CONFIG FILE FOR STREAMING SPARK METRICS
  echo "init_config:
instances:
    - resourcemanager_uri: http://\$DB_DRIVER_IP:\$DB_DRIVER_PORT
      spark_cluster_mode: spark_standalone_mode
      cluster_name: \$current" > /etc/datadog-agent/conf.d/spark.yaml

  # RESTARTING AGENT
  sudo service datadog-agent restart

fi
EOF

# CLEANING UP
if [ $DB_IS_DRIVER ]; then
  chmod a+x /tmp/start_datadog.sh
  /tmp/start_datadog.sh >> /tmp/datadog_start.log 2>&1 & disown
fi
