hadoop:
  user.present:
    - shell: /bin/bash
    - home:  /home/hadoop
    - groups: 
      - hadoop
    - require:
      - group: hadoop
  group.present:
    - system: True
  archive:
    - extracted
    - name: /usr/local
    - source: salt://hadoop/hadoop-1.2.1.tar.gz
    - archive_format: tar
    - tar_options:  
    - if_missing: /usr/local/hadoop-1.2.1
    - require:
      - group: hadoop
      - user: hadoop

/usr/local/hadoop:
#  file.directory:
#    - user: hadoop
#    - group: hadoop
#    - makedirs: True
#    - recurse:
#      - user
#      - group
#    - require:
#      - group: hadoop
#      - user: hadoop
  file.symlink:
    - force: True
    - target: /usr/local/hadoop-1.2.1
    - user: hadoop
    - group: hadoop
    - require:
      - group: hadoop
      - user: hadoop

/usr/local/hadoop-1.2.1:
  file.directory:
    - user: hadoop
    - group: hadoop
    - makedirs: True
    - recurse:
      - user
      - group
    - require:
      - group: hadoop
      - user: hadoop
