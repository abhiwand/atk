rm /etc/yum.repos.d/gaoPrivate.repo:
  cmd.run

rm /etc/yum.repos.d/gaoRelease.repo:
  cmd.run

/etc/yum.repos.d/ia.repo:
  file.managed:
    - source: salt://cleanVM/jinja-gaoPrivate.repo
    - template: jinja
    - defaults:
      baseUrl: https://s3-us-west-2.amazonaws.com/intel-analytics-repo/release/0.8.0/yum/dists/rhel/6
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
 
history -c && history -w; echo "cleared root user history":
  cmd.run:
    - user: root

history -c && history -w; echo "cleared cloudera user history":
  cmd.run:
    - user: cloudera    
