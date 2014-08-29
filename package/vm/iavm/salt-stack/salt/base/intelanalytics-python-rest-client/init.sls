intelanalytics-python-rest-client:
  pkg.latest:
    - refresh: True
    - require:
      - pkg: yum-s3
#      - sls: gaoPrivateRepo


/home/cloudera/Documents/quick-hadoop:
  file.recurse:
    - source: salt://intelanalytics-python-rest-client/VM_Page
    - makedirs: True
    - user: cloudera
    - group: cloudera

mkdir -p /home/cloudera/Documents/quick-hadoop/python:
  cmd.run

cp -Rv /usr/lib/intelanalytics/rest-client/python/doc/html/* /home/cloudera/Documents/quick-hadoop/python:
  cmd.run

chown -R cloudera:cloudera /home/cloudera:
  cmd.run
