intelanalytics-rest-server:
  pkg.latest:
    - refresh: True
    - require:
      - pkg: yum-s3
      - sls: gaoPrivateRepo

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

/etc/intelanalytics/rest-server/reference.conf:
  file.managed:
    - source: salt://intelanalytics-rest-server/reference.conf
    - required: 
      - pkg: intelanalytics-rest-server

/etc/default/intelanalytics-rest-server:
  file.managed:
    - source: salt://intelanalytics-rest-server/intelanalytics-rest-server
    - required:
      - pkg: intelanalytics-rest-server

chkconfig intelanalytics-rest-server on:
  cmd.run:
    - required:
      - pkg: intelanalytics-rest-server

/home/iauser/datasets:
  file.recurse:
    - source: salt://intelanalytics-rest-server/examples/datasets
    - required:
      - pkg: intelanalytics-rest-server

/home/cloudera/examples:
  file.recurse:
    - source: salt://intelanalytics-rest-server/examples
    - required:
      - pkg: intelanalytics-rest-server
    
su -c "hadoop fs -put /home/iauser/datasets " iauser:
  cmd.run

