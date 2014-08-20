base:
  '*':
    - gaoPublicRepo
    - yum-s3
    - gaoPrivateRepo
    - intelanalytics-rest-server
    - intelanalytics-graphbuilder
    - intelanalytics-python-rest-client
  '*prod*':
    - cleanVM
