base:
  '*-Admin':
    - saltJumpNode
    - yum-s3-plugin
    - bdarepo
    - intelanalytics-aws
  'dockerrepo':
    - pythonBuild
    - dockerRepo
    - nginx
