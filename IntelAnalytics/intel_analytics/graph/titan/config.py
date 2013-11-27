from time import strftime
import os

__all__ = [ 'titan_config']

class TitanConfig(object):
    from intel_analytics.config import global_config

    def __init__(self, config=global_config):
        self.config = config

    def write_gb_cfg(self, tablename, stream=None):
        """
        Writes a GraphBuilder config XML file

        Parameters
        ----------
        tablename : string
            name of destination table in Titan
        stream : stream
           an open dest stream, if None, cfg written to cfg-specified file path

        Returns
        -------
        filename : string
            full path of the config file created
        """
        self.config['titan_storage_tablename'] = tablename
        filename = os.path.join(self.config['conf_folder'],
                                "graphbuilder_" + tablename+".xml")
        return self._write_cfg(tablename,
                               stream,
                               filename,
                               gb_keys,
                               self._write_gb)

    def write_rexster_cfg(self, tablename, stream=None):
        """
        Writes a Rexster config XML file

        Parameters
        ----------
        tablename : string
           name of destination table in Titan
        stream : stream
           an open dest stream, if None, cfg written to cfg-specified file path

        Returns
        -------
        filename : string
            full path of the config file created
        """
        return self._write_cfg(tablename,
                               stream,
                               self.config['rexster_xml'],
                               rexster_keys,
                               self._write_rexster)


    def _write_cfg(self, tablename, stream, filename, keys, func):
        if tablename is None: raise Exception("tablename is None")

        self.config.verify(keys)

        if stream is not None:
            func(stream)
            return ""
        else:
            with open(filename, 'w') as out:
                func(out)
            return filename

    def _write_gb(self, out):
        _write_header(out, "GraphBuilder")
        out.write("<configuration>\n")
        for k in gb_keys:
            _write_gb_prop(out, k, self.config[k])
        out.write("</configuration>\n")

    def _write_rexster(self, out):
        _write_header(out, "Rexster")
        out.write(rexster_template.substitute(self.config))


def _write_header(out, cfg_name):
    out.write("<!-- ")
    out.write(cfg_name)
    out.write(strftime(" cfg file generated at %Y-%m-%d %H:%M:%S-->\n\n"))

def _write_gb_prop(out, name, value):
    out.write("  <property>\n    <name>graphbuilder.")
    out.write(name)
    out.write("</name>\n    <value>")
    out.write(value)
    out.write("</value>\n  </property>\n")

#--------------------------------------------------------------------------
# Rexster
#--------------------------------------------------------------------------
rexster_template_str = """
 <rexster>
    <http>
        <server-port>8182</server-port>
        <server-host>${rexster_serverhost}</server-host>
        <base-uri>${rexster_baseuri}</base-uri>
        <web-root>public</web-root>
        <character-set>UTF-8</character-set>
        <enable-jmx>false</enable-jmx>
        <enable-doghouse>true</enable-doghouse>
        <max-post-size>2097152</max-post-size>
        <max-header-size>8192</max-header-size>
        <upload-timeout-millis>30000</upload-timeout-millis>
        <thread-pool>
            <worker>
                <core-size>8</core-size>
                <max-size>8</max-size>
            </worker>
            <kernal>
                <core-size>4</core-size>
                <max-size>4</max-size>
            </kernal>
        </thread-pool>
        <io-strategy>leader-follower</io-strategy>
    </http>
    <rexpro>
        <server-port>8184</server-port>
        <server-host>${rexster_serverhost}</server-host>
        <session-max-idle>1790000</session-max-idle>
        <session-check-interval>3000000</session-check-interval>
        <connection-max-idle>180000</connection-max-idle>
        <connection-check-interval>3000000</connection-check-interval>
        <enable-jmx>false</enable-jmx>
        <thread-pool>
            <worker>
                <core-size>8</core-size>
                <max-size>8</max-size>
            </worker>
            <kernal>
                <core-size>4</core-size>
                <max-size>4</max-size>
            </kernal>
        </thread-pool>
        <io-strategy>leader-follower</io-strategy>
    </rexpro>
    <shutdown-port>8183</shutdown-port>
    <shutdown-host>${rexster_serverhost}</shutdown-host>
    <script-engines>
        <script-engine>
            <name>gremlin-groovy</name>
            <reset-threshold>-1</reset-threshold>
            <init-scripts>config/init.groovy</init-scripts>
            <imports>com.tinkerpop.rexster.client.*</imports>
            <static-imports>java.lang.Math.PI</static-imports>
        </script-engine>
    </script-engines>
    <security>
        <authentication>
            <type>none</type>
            <configuration>
                <users>
                    <user>
                        <username>rexster</username>
                        <password>rexster</password>
                    </user>
                </users>
            </configuration>
        </authentication>
    </security>
    <metrics>
        <reporter>
            <type>jmx</type>
        </reporter>
        <reporter>
            <type>http</type>
        </reporter>
        <reporter>
            <type>console</type>
            <properties>
                <rates-time-unit>SECONDS</rates-time-unit>
                <duration-time-unit>SECONDS</duration-time-unit>
                <report-period>10</report-period>
                <report-time-unit>MINUTES</report-time-unit>
                <includes>http.rest.*</includes>
                <excludes>http.rest.*.delete</excludes>
            </properties>
        </reporter>
    </metrics>
    <graphs>
        <graph>
            <graph-name>${titan_storage_tablename}</graph-name>
            <graph-type>com.thinkaurelius.titan.tinkerpop.rexster.TitanGraphConfiguration</graph-type>
            <graph-location></graph-location>
            <graph-read-only>false</graph-read-only>
            <properties>
                <storage.backend>${titan_storage_backend}</storage.backend>
                <storage.hostname>${titan_storage_hostname}</storage.hostname>
                <storage.port>${titan_storage_port}</storage.port>
                <storage.tablename>${titan_storage_tablename}</storage.tablename>
            </properties>
            <extensions>
                <allows>
                        <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>
    </graphs>
</rexster>
"""

from string import Template
from collections import defaultdict
rexster_template = Template(rexster_template_str)

# pull the required keys from the template
d = defaultdict(lambda : None)
rexster_template.substitute(d)
rexster_keys = d.keys()
rexster_keys.sort()

#--------------------------------------------------------------------------
# GraphBuilder
#--------------------------------------------------------------------------
gb_keys = ['conf_folder']
for k in ['hostname', 'tablename', 'backend', 'port', 'connection_timeout']:
    gb_keys.append('titan_storage_' + k)
gb_keys.sort()


titan_config = TitanConfig()
