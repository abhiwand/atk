intelanalytics-aws:
  pkg.installed:
    - fromrepo: bda
    - refresh: True
    - pkgs: 
      - intelanalytics-aws
    - require:
      - pkg: yum-s3-plugin
