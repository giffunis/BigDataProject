FROM cbarrancoupo/hbase-pseudo

COPY entrypoint.sh /
RUN chown root:root entrypoint.sh && chmod 775 entrypoint.sh

RUN mkdir -p /bigdata
COPY BigData.jar /bigdata
RUN chown root:root /bigdata/BigData.jar && chmod 775 /bigdata/BigData.jar

COPY bigdata /usr/local/bin
RUN chown root:root /usr/local/bin/bigdata && chmod 775 /usr/local/bin/bigdata

ENTRYPOINT ["/entrypoint.sh"]
