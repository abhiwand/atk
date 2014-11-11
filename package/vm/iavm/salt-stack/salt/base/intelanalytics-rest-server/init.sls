intelanalytics-rest-server:
  pkg.latest:
    - refresh: True
    - pkgs:
      - intelanalytics-rest-server
    - require:
      - pkg: yum-s3
#      - sls: gaoPrivateRepo

ls -l /etc/intelanalytics/rest-server:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server

chmod +x config:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server
    - cwd: /etc/intelanalytics/rest-server/

config --host localhost --port 7180 --username cloudera --password cloudera --cluster "Cloudera QuickStart - C5" --restart yes --python python --db-host localhost --db-port 5432 --db ia_metastore --db-username iauser --db-password iauser --db-skip-reconfig no:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server
    - cwd: /etc/intelanalytics/rest-server/


/etc/intelanalytics/rest-server/application.conf:
  file.managed:
    - source: salt://intelanalytics-rest-server/application.conf
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



    

