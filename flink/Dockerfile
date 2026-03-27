FROM flink:1.17.2

RUN wget -q -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar && \
    wget -q -P /opt/flink/lib/ \
      https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar
