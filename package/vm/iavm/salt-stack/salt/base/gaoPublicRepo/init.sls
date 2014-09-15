/etc/yum.repos.d/ia-public.repo:
  file.managed:
    - source: salt://gaoPublicRepo/jinja-gaoPublic.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['public']['baseUrl'] }}

yum clean all:
  cmd.run