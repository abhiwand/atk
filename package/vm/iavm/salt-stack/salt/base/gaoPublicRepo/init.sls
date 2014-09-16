/etc/yum.repos.d/ia-public.repo:
  file.managed:
    - source: salt://gaoPublicRepo/jinja-gaoPublic.repo
    - template: jinja
    - defaults:
      baseUrl: {{ pillar['gaoRepo']['public']['baseUrl'] }}

echo "public";yum clean all:
  cmd.run