yum-s3:
  pkg.latest:
    - refresh: True
#    - sources:
#      - yum-s3: salt://yum-s3-plugin/yum-s3-0.2.4-1.noarch.rpm

/etc/yum/pluginconf.d/security.conf:
  file.managed:
    - source: salt://yum-s3/security.conf
    - required: 
      pkg: yum-s3

