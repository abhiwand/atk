intelanalytics-rest-server:
  pkg.latest:
    - refresh: True
    - pkgs:
      - intelanalytics-rest-server
    - require:
      - pkg: yum-s3
#      - sls: gaoPrivateRepo


/etc/intelanalytics/rest-server/application.conf:
  file.managed:
    - source: salt://intelanalytics-rest-server/application.conf
    - required: 
      - pkg: intelanalytics-rest-server

ls -l /etc/intelanalytics/rest-server:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server

chmod +x config.py:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server
    - cwd: /etc/intelanalytics/rest-server/

python config.py --host localhost --port 7180 --username cloudera --password cloudera --cluster "Cloudera QuickStart - C5" --restart yes:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server
    - cwd: /etc/intelanalytics/rest-server/

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
    

