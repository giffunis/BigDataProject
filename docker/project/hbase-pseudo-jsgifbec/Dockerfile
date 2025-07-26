FROM cbarrancoupo/hbase-pseudo

COPY entrypoint.sh /

RUN chown root:root entrypoint.sh && chmod 775 entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
