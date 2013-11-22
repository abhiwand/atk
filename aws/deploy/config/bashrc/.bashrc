# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

if [ -f ${HOME}/IntelAnalytics/bin/IntelAnalytics_env.sh ]; then
    . ${HOME}/IntelAnalytics/bin/IntelAnalytics_env.sh
fi

