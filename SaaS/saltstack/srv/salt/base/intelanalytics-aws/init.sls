intelanalytics-aws:
  pkg.latest:
    - fromrepo: bda
    - refresh: True
    - pkgs: 
      - intelanalytics-aws
    - require:
      - pkg: yum-s3-plugin

/home/ec2-user/intel/IntelAnalytics:
  file.directory:
    - user: ec2-user
    - group: ec2-user
    - recurse:
      - user
      - group
    - require:
      - pkg: intelanalytics-aws

