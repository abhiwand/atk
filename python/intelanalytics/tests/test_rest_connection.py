##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

import iatest
iatest.init()
#iatest.set_logging("intelanalytics.rest.connection", 20)

import unittest
from mock import patch, Mock, MagicMock
from intelanalytics.rest.iaserver import IaServer


conn1 = {'host': 'localhost', 'port': 9099, 'scheme': 'http', 'version': 'v1'}

def create_conn1():
    return IaServer()


# class TestRestConnection(unittest.TestCase):
#
#     def test_connection_creation(self):
#         c = IaServer()
#         self.assertEquals(conn1['host'], c.host)
#         self.assertEquals(conn1['port'], c.port)
#         self.assertEquals(conn1['scheme'], c.scheme)
#         self.assertEquals(conn1['version'], c.version)
#
#     def test_connection_get_scheme_and_authority(self):
#         c = IaServer()
#         expected = 'http://localhost:9099'
#         self.assertEquals(expected, c._get_scheme_and_authority())
#
#     @patch('intelanalytics.rest.connection.requests')
#     def test_valid_ping(self, mock_requests):
#         mock_response = Mock()  # todo - create a good mock_requests
#         mock_response.json.return_value = {'name': 'Intel Analytics'}
#         mock_requests.get.return_value = mock_response
#         c = create_conn1()
#         c.ping()
#
#     def bad_ping_conn2(self):
#         try:
#             create_conn1().ping()
#         except Exception as e:
#             self.assertTrue(str(e).startswith("Failed to ping Intel Analytics at http://localhost:"))
#         else:
#             self.fail()
#
#     @patch('intelanalytics.rest.connection.requests')
#     def test_invalid_ping(self, mock_requests):
#         mock_requests.get.side_effect = Exception("Expected Exception")
#         self.bad_ping_conn2()
#
# '''
#     @patch('intelanalytics.rest.connection.requests')
#     def test_invalid_ping_bad_info(self, mock_requests):
#         mock_response = Mock()
#         mock_response.json.return_value = {'name': 'Intel Aardvarks'}
#         mock_requests.get.return_value = mock_response
#         self.bad_ping_conn1()
#         '''


class MockRequests(MagicMock):

    def get(self, uri, **kwargs):
        response = Mock()
        response.uri = uri
        response.text = uri
        return response

    def delete(self, uri, headers=None):
        response = Mock()
        response.uri = uri
        response.text = uri
        return response


    def post(self, *args, **kwargs):
        response = Mock()
        response.text = ""
        response.args = args
        response.kwargs = kwargs
        return response



# class TestHttpMethods(unittest.TestCase):
#
#     def init_mock_conn_and_return_uri(self, mock_conn):
#         base_uri, version, path = "http://good:7", "v08", "bigtime/on/my/way"
#         mock_conn.version = version
#         mock_conn.get_base_uri.side_effect = lambda: "%s/%s/" % (base_uri, version)
#         return path, '/'.join([base_uri, version, path])
#
#     @patch('intelanalytics.rest.connection.requests', new=MockRequests())
#     @patch('intelanalytics.rest.connection.http.server')
#     def test_get(self, mock_conn):
#         path, uri = self.init_mock_conn_and_return_uri(mock_conn)
#         r = http.get(path)
#         self.assertEquals(uri, r.uri)
#
#     @patch('intelanalytics.rest.connection.requests', new=MockRequests())
#     @patch('intelanalytics.rest.connection.http.server')
#     def test_delete(self, mock_conn):
#         path, uri = self.init_mock_conn_and_return_uri(mock_conn)
#         r = http.delete(path)
#         self.assertEquals(uri, r.uri)
#
#     @patch('intelanalytics.rest.connection.requests', new=MockRequests())
#     @patch('intelanalytics.rest.connection.http.server')
#     def test_post(self, mock_conn):
#         path, uri = self.init_mock_conn_and_return_uri(mock_conn)
#         payload = { 'a': 'aah', 'b': 'boo', 'c': 'caw'}
#         r = http.post(path, payload)
#         self.assertEquals(uri, r.args[0])
#         self.assertEquals(json.dumps(payload), r.kwargs['data'])
#         self.assertTrue(r.kwargs['headers'])
#
# if __name__ == '__main__':
#     unittest.main()
