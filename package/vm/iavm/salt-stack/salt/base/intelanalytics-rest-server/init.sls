intelanalytics-rest-server:
  pkg.latest:
    - refresh: True
    - require:
      - pkg: yum-s3
#      - sls: gaoPrivateRepo


/etc/intelanalytics/rest-server/application.conf:
  file.managed:
    - source: salt://intelanalytics-rest-server/application.conf
    - required: 
      - pkg: intelanalytics-rest-server

ls -l /etc/intelanalytics:
  cmd.run:
    - required: 
      - pkg: intelanalytics-rest-server

ls -l /etc/intelanalytics/rest-server:
  cmd.run:
    - required: 
      - pkg: intelanalytics-rest-server

/etc/security/limits.conf:
  file.managed:
    - source: salt://intelanalytics-rest-server/limits.conf
    - user: root
    - group: root
    - mode: 644
    - required: 
      - pkg: intelanalytics-rest-server

chkconfig intelanalytics on:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server


cp -Rv /usr/lib/intelanalytics/rest-server/examples /home/cloudera/examples:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server

chmown -R cloudera:cloudera /home/cloudera:
  cmd.run
    

