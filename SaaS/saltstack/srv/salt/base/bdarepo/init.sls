/etc/yum.repos.d/bda.repo:
  file.managed:
    - source: salt://bdarepo/jinja-bda.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['bdaRepo']['baseUrl'] }}
      repoType: {{ pillar['bdaRepo']['repoType'] }}
      sprint: {{ pillar['bdaRepo']['sprint'] }}
      keyId: {{ pillar['bdaRepo']['keyId'] }}
      secretKey: {{ pillar['bdaRepo']['secretKey'] }}
    - require: 
      - pkg: yum-s3-plugin
  
