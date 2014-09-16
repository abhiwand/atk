/etc/yum.repos.d/gaoRelease.repo:
  file.managed:
    - source: salt://gaoReleaseRepo/jinja-gaoRelease.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['pre-release']['baseUrl'] }} 
      keyId: {{ pillar['gaoRepo']['pre-release']['keyId'] }}
      secretKey: {{ pillar['gaoRepo']['pre-release']['secretKey'] }}
    - require: 
      - pkg: yum-s3

echo "pre-release";yum clean all:
  cmd.run

yum search intelanalytics:
  cmd.run

