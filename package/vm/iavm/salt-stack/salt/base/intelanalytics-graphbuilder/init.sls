intelanalytics-graphbuilder:
  pkg.latest:
    - refresh: True
    - require:
      - pkg: yum-s3
#      - sls: gaoPrivateRepo

/usr/lib/intelanalytics/graphbuilder/set-cm-spark-classpath.sh --interactive false --host localhost --port 7180 --username cloudera --password cloudera --clustername "Cloudera QuickStart - C5" --restart yes:
  cmd.run
