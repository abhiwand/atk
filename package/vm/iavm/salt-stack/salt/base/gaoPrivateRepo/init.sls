/etc/yum.repos.d/gaoPrivate.repo:
  file.managed:
    - source: salt://gaoPrivateRepo/jinja-gaoPrivate.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['private']['baseUrl'] }}
      branch: {{ pillar['gaoRepo']['private']['branch'] }}
      keyId: {{ pillar['gaoRepo']['private']['keyId'] }}
      secretKey: {{ pillar['gaoRepo']['private']['secretKey'] }}
    - require: 
      - pkg: yum-s3

yum clean all:
  cmd.run

yum search intelanalytics:
  cmd.run

ls -la /etc/yum.repos.d:
  cmd.run

cat /etc/yum.repos.d/gaoPrivate.repo:
  cmd.run
