##############################################################################
## INTEL CONFIDENTIAL
##
## Copyright 2013 Intel Corporation All Rights Reserved.
##
## The source code contained or described herein and all documents related to
## the source code (Material) are owned by Intel Corporation or its suppliers
## or licensors. Title to the Material remains with Intel Corporation or its
## suppliers and licensors. The Material may contain trade secrets and
## proprietary and confidential information of Intel Corporation and its
## suppliers and licensors, and is protected by worldwide copyright and trade
## secret laws and treaty provisions. No part of the Material may be used,
## copied, reproduced, modified, published, uploaded, posted, transmitted,
## distributed, or disclosed in any way without Intel's prior express written
## permission.
##
## No license under any patent, copyright, trade secret or other intellectual
## property right is granted to or conferred upon you by disclosure or
## delivery of the Materials, either expressly, by implication, inducement,
## estoppel or otherwise. Any license under such intellectual property rights
## must be express and approved by Intel in writing.
##############################################################################

import requests
import json

def main(base):
    print "Existing frames: ", requests.get(base + "/dataframes").json()
    print "Create a frame..."
    spec = {'name': "myframe", 'schema': {'columns':[('id', 'int'), ('foo', 'string')]}}
    res = requests.post(base + "/dataframes", data = json.dumps(spec), headers = {'Content-Type':'application/json'})
    print res.text
    frame = res.json()
    selfUrl = [u['uri'] for u in frame['links'] if u['rel'] == 'self']
    if not selfUrl:
        print "No self URL, that's strange..."
    else:
        print "Retrieve the same frame using the self URL:"
        print requests.get(selfUrl[0]).text

if __name__ == '__main__':
    main("http://localhost:8080/v1")