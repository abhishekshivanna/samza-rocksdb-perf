## Deployment

###Useful commands
```bash

export JAVA_HOME=`readlink -f /usr/bin/java | sed "s:/jre/bin/java::"`
./sbin/yarn-daemon.sh stop resourcemanager
./sbin/yarn-daemon.sh start resourcemanager
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${PWD}/config/config.properties
```