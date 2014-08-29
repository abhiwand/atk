rm /etc/yum.repos.d/gaoPrivate.repo:
  cmd.run

/etc/yum.repos.d/ia.repo:
  file.managed:
    - source: salt://cleanVM/jinja-gaoPrivate.repo
    - template: jinja
    - defaults:
      baseUrl: http://
      keyId: myKey
      secretKey: mySecret
    - require: 
      - pkg: intelanalytics-rest-server
      - pkg: intelanalytics-graphbuilder

echo "" > /etc/environment:
  cmd.run:
    - require:
      - pkg: intelanalytics-rest-server
      - pkg: intelanalytics-graphbuilder

/home/cloudera/.bashrc:
  file.managed:
    - source: salt://cleanVM/.bashrc
    - user: cloudera
    - group: cloudera
    - mode: 644
    - require:
      - pkg: intelanalytics-rest-server
      - pkg: intelanalytics-graphbuilder
 
