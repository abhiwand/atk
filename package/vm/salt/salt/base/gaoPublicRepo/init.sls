/etc/yum.repos.d/gaoPublic.repo:
  file.managed:
    - source: salt://gaoPublicRepo/jinja-gaoPublic.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['public']['baseUrl'] }}
