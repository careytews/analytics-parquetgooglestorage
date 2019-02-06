FROM fedora:26

RUN dnf install -y libgo

COPY parquetgooglestorage /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/parquetgooglestorage"]
CMD [ "/queue/input" ]


