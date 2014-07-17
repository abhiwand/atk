echo '
export HTTP_PROXY="http://proxy.jf.intel.com:911"
export HTTPS_PROXY="http://proxy.jf.intel.com:911"
export http_proxy="http://proxy.jf.intel.com:911"
export https_proxy="http://proxy.jf.intel.com:911"
export no_proxy="hf.intel.com,jf.intel.com,intel.com,192.168.0.0/16,localhost,10.54.10.141"
export NO_PROXY="hf.intel.com,jf.intel.com,intel.com,192.168.0.0/16,localhost,10.54.10.141"
export SOCKS_PROXY=socks://proxy-socks.jf.intel.com:1080
export socks_proxy=socks://proxy-socks.jf.intel.com:1080
' >> .bashrc

echo '
HTTP_PROXY="http://proxy.jf.intel.com:911"
HTTPS_PROXY="http://proxy.jf.intel.com:911"
http_proxy="http://proxy.jf.intel.com:911"
https_proxy="http://proxy.jf.intel.com:911"
no_proxy="hf.intel.com,jf.intel.com,intel.com,192.168.0.0/16,localhost,10.54.10.141"
NO_PROXY="hf.intel.com,jf.intel.com,intel.com,192.168.0.0/16,localhost,10.54.10.141"
SOCKS_PROXY=socks://proxy-socks.jf.intel.com:1080
socks_proxy=socks://proxy-socks.jf.intel.com:1080' | sudo tee -a /etc/environment

#echo 'Defaults     env_keep += "http_proxy HTTP_PROXY https_proxy HTTPS_PROXY no_proxy NO_PROXY SOCKS_PROXY socks_proxy"' | sudo tee -a /etc/sudoers 
cat .bashrc
sudo cat /etc/environment

#git config --global url."https://".insteadOf git://
#git config --global http.proxy %HTTP_PROXY%
#git config --global https.proxy %HTTP_PROXY%


#sudo bootstrap-salt.sh -I git develop

