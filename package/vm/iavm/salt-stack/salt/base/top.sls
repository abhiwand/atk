base:
  '*':
    - gaoPublicRepo
    - yum-s3
    - gaoPrivateRepo
    - intelanalytics-rest-server
    - intelanalytics-graphbuilder
  '*prod*':
    - cleanVM
