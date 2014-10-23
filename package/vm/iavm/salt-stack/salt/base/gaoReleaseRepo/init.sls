/etc/yum.repos.d/gaoRelease.repo:
  file.managed:
    - source: salt://gaoReleaseRepo/jinja-gaoRelease.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['release']['baseUrl'] }} 
      keyId: {{ pillar['gaoRepo']['release']['keyId'] }}
      secretKey: {{ pillar['gaoRepo']['release']['secretKey'] }}
    - require: 
      - pkg: yum-s3

echo "release"; yum clean all:
  cmd.run

yum search intelanalytics:
  cmd.run

ls -la /etc/yum.repos.d:
  cmd.run

cat /etc/yum.repos.d/gaoRelease.repo:
  cmd.run
