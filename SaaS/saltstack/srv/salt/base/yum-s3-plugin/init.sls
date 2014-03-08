yum-s3-plugin:
  pkg.installed:
    - sources:
      - yum-s3: salt://yum-s3-plugin/yum-s3-0.2.4-1.noarch.rpm

/etc/yum/pluginconf.d/security.conf:
  file.managed:
    - source: salt://yum-s3-plugin/security.conf
    - required: 
      pkg: yum-s3-plugin

