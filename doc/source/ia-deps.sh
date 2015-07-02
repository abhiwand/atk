echo "[taproot-analytics-deps]
name=taproot-analytics-deps
baseurl=https://taproot-analytics-dependencies.s3-us-west-2.amazonaws.com/yum
gpgcheck=0
priority=1 enabled=1"  | sudo tee -a /etc/yum.repos.d/ia-deps.repo

