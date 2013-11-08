# we parse all the fields below, but in the pig context we should only use the fields we are interested in
#TODO: need to determine these
@outputSchema("sip: chararray, rpr: chararray, sn: chararray, pktsz: long, bufsz: long, id: chararray")
def parseRecord(line):
    #TODO: may need to do some type checks here with the schema that the user specifies and the parsed values
    #otherwise we get class cast exceptions from pig with type mismatches
      return parseRawRecord(line)
  
FIELDS = [
    'sip' , # source ip
    'rpr' , # protocol type
    'sn' , # server name
    'pktsz' , # packet size
    'bufsz' , # buffer size
    'cuuid' , # for SSL queries only, a uuid for the connection
    'id' , # Query id on repper server that answered the query 
    'sercert' , # Serial number, base64 encoded; this has to match the serial in the certificate for UTT connections; see also TS SDK Meta Data Fields article. This can be a unique serial number or a shared serial number (e.g. by a whole product family
    'devid' , # Device id, base64 encoded; the device serial number goes here; see also TS SDK Meta Data Fields article. This field should be a unique value to the installation sending the query (such as the serial number of the integrating product or a constant randomly generated UUID). 
    'cliid' , # Client id, base64 encoded; opaque TS SDK data (this data is added by the TS SDK and logged only; its purpose it unknown) 
    'prn' , #Product name, base64 encoded 
    'prv' , #Product version, base64 encoded 
    'art' , #key
    'amcdet' , #Q_AMCORE_DETECTION, detection name and content version 
    'ampinf' , #Q_AMCORE_PRODINFO, product ID and content versions 
    'unk' , # don't know this, we see this in the dataset, but Ram doesn't mention this field in his email
    #unk may appear multiple time in a record, need to handle this carefully
    'amcfd' , #Q_AMCORE_FEATDATA, a set of integer values representing frequency of a feature identified. Effectively Boolean. 
    'flags' , #Flag field submitted by client
    'ct' , #Chunk type, unsigned integer. Each query is encapsulated in a chunk. All TS SDK-generated queries have chunk type 9. Older Sidewinder connection reputation lookups (using librep as client library) use chunk type 4. 
    'seq' , #Sequence number in query packet, an unsigned integer value 
    'artres' ,#Artemis result sent back to client encoded as an unsigned long IP 
    'uchd',#the number of cache hits from the client SDK based on direct URL matches
    'uchi',#: the number of cache hits from the client SDK based on indirect URL matches 
    'catset', #Category set in query (for URL lookups) 
    'sid',#Session ID (for URL lookups) 
    
    ################################################################################
    #URL categorization request; can occur multiple times; the value to this key is a comma separated list: 
    #the fields of urlcat are:
    'urlcat',
    
    'url',#base64 encoded (from client) 
    'host_header',#base64 encoded (from client) 
    'flags',#unsigned integer (from client) 
    'ip_list',#semicolon separated list of IPv6 addresses in presentation form that the host name in the URL resolves to (from client) 
    'cat_list',#semicolon separated list of unsigned integer category IDs 
    'reputation',#reputation for URL, signed integer (returned to client) 
    'url_flags',#flags for URL (returned to client) 
    ################################################################################33
    
    'url_rgx',#A regex that matched the preceding URL for ts_regex score/category modification. 
    
    ################################################################################
    #Connection reputation lookup data, a comma separated list containing the following fields
    'conn',
    #### then, connection reputation lookup data
    #I made these keys up for storing the relevant info into dictionary
    'conn_src_ip', #Source IP in integer representation (from client) 
    'conn_src_port',#Source port (from client) 
    'conn_dest_ip',#Destination IP in integer representation (from client
    'conn_dest_port',#Destination Port (from client) 
    'conn_transport_protocol',#Transport protocol, unsigned integer (from client) 
    'conn_flags', #Flags, unsigned integer (from client) 
    'conn_src_ip_reputation',#Source IP reputation, signed integer, empty if not requested by client (returned to client) 
    'conn_dest_ip_reputation', #Destination IP reputation, signed integer, empty if not requested by client (returned to client) 
    ################################################################################
    
    'rusec',# response time in microsec
    'amfcts',#the file creation date 
    'ro',#Read only query if non-zero 
    'urlcache',#The cacheable URL returned to TS SDK 
    
    ############## Casper Data ##############
    'amosvmm',#which operating system 
    'amcdet', #Q_AMCORE_DETECTION, detection name and content version 
    'amcdigsig', #Q_AMCORE_DIGSIGN, certificate issuer,subject, expiration, and sha1 of public key. Note this information is all public. 
    'amcgh3', #Q_AMCORE_GEOHASH3, fingerprint of a PE file with calculated values from the header and entropy of sections of the document. 
    'amcgh6', #Q_AMCORE_GEOHASH6, fingerprint of a PE file with calculated values from the header and entropy of sections of the document. 
    'amcrsig', #Q_AMCORE_RSIG, fingerprint of a PE file with calculated values from the resources of the file (icon, timestamp, etc)
    'amcli', #Q_AMCORE_LINKERINFO, fingerprint of a PE file  an md5 of the linker info section 
    'amcimp', #Q_AMCORE_IMPORT, fingerprint of a PE file an md5 of the import section 
    'amcfd', #Q_AMCORE_FEATDATA, a set of integer values representing frequency of a feature identified. Effectively Boolean. 
    'ampmd5', #Q_AMCORE_PARENT_MD5, the md5 of the file which is the main module of the process that created the 'child file' 
    'amconm', #Q_AMCORE_COMPANY_NAME, company name of a file extracted from the binary's version info. E.g. 'Microsoft'
    'amprnm', #Q_AMCORE_PROD_NAME, company name of a file extracted from the binary's version info. E.g. 'Windows' 
    'amosvmm', #Q_AMCORE_OS_VER_MAJ_MIN, which operating system 
    'amsvcnm', #Q_AMCORE_SVC_NAME, the name of the service as installed. E.g. 'mfecore' 
    'ampehts', #Q_AMCORE_PE_HDR_TS, the timestamp from the pe header 
    'amfcts', #Q_AMCORE_FILE_CREATE_TS, the file creation date 
    'ampinf', #Q_AMCORE_PRODINFO, product ID and content versions 

    ############## File Data ##############
    'art', #Artemis hash, base64 encoded 
    'artres', #Artemis result sent back to client encoded as an unsigned long IP 
    'filepath', #Artemis file name, base64 encoded 
    'artsha1', #Data we get from field 64 aka Q_ARTEMIS_SHA1 
    'artsha256', #Data we get from field 64 aka Q_ARTEMIS_SHA256 
    'artqry', #An Artemis DNS query (the queried name); DNS-encoded, base64-encoded     

]

class MCAfeeDataParsingException(Exception):
    pass

def parseConnectionString(connection_string, parsed):
    if connection_string != None:
        connection_string_cols = connection_string.split(',')
        try:
            parsed['conn_src_ip'] = connection_string_cols[0]
            parsed['conn_src_port'] = connection_string_cols[1]
            parsed['conn_dest_ip'] = connection_string_cols[2]
            parsed['conn_dest_port'] = connection_string_cols[3]
            parsed['conn_transport_protocol'] = connection_string_cols[4]
            parsed['conn_flags'] = connection_string_cols[5]
            parsed['conn_src_ip_reputation'] = connection_string_cols[6]
            parsed['conn_dest_ip_reputation'] = connection_string_cols[7]        
        except:
            pass    
        
def parseUrlCategoryString(urlcat_string, parsed):
    if urlcat_string != None:
        urlcat_string_cols = urlcat_string.split(',')
        try:
            parsed['url'] = urlcat_string_cols[0]
            parsed['host_header'] = urlcat_string_cols[1]
            parsed['flags'] = urlcat_string_cols[2]
            parsed['ip_list'] = urlcat_string_cols[3]
            parsed['cat_list'] = urlcat_string_cols[4]
            parsed['reputation'] = urlcat_string_cols[5]
            parsed['url_flags'] = urlcat_string_cols[6]
        except:
            pass    
          
def parseRawRecord(line):
    cols = line.split()
    unix_ts = cols[0]
    parsed = {}
    for col in cols[1:]:
        key_val = col.split(':')
        key = key_val[0]
        value = key_val[1]
        if key not in FIELDS:
            print "LINE: ", line
            raise MCAfeeDataParsingException("WE ARE NOT HANDLING KEY: " + key + " NEED TO ADD THIS KEY TO MCAFEE_UDFS!")
        parsed[key] = value
                
    #parse comma separated internal structures
    connection_string = parsed.pop("conn", None)
    parseConnectionString(connection_string, parsed)
    
    urlcat_string = parsed.pop("urlcat", None)
    parseUrlCategoryString(urlcat_string, parsed)
            
    ##handle type conversions
    pktsz = parsed.get('pktsz', None)
    if pktsz == None:
        parsed['pktsz'] = pktsz
    else:
        parsed['pktsz'] = long(pktsz)
        
    bufsz = parsed.get('bufsz', None)
    if bufsz == None:
        parsed['bufsz'] = bufsz
    else:
        parsed['bufsz'] = long(bufsz)   
                    
    output=[]
    for field in FIELDS:
        output.append(parsed.get(field, None))
    return tuple(output)

def main():
    file = open('/tmp/mcafee_ram_data.txt')
#     file = open('/tmp/mcafee_single.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print parseRawRecord(line)
        print
    file.close()
    
if __name__ == "__main__":
    main()