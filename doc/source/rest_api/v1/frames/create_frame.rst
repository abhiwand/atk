===============
Frames REST API
===============

----------------
Creating a Frame
----------------

POST /v1/frames/
================

Request
-------

**Route** ::

  POST /v1/frames/

**Body**

+-------------------------------+----------------------------------------------+-----------+-----------------------------+------------------+
| Name                          | Description                                  | Default   | Valid Values                |  Example Values  |
+===============================+==============================================+===========+=============================+==================+
| name                          | name for the frame                           | null      | alphanumeric UTF-8 strings  | 'weather_frame1' |
+-------------------------------+----------------------------------------------+-----------+-----------------------------+------------------+

::

  {
    "name": "weather_frame1"
 }

**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json

Response
--------

**Status** ::

  200 OK

**Body**

Returns a summary of the frame.

+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| id                            | frame id (engine-assigned)                   |
+-------------------------------+----------------------------------------------+
| name                          | frame name (user-assigned)                   |
+-------------------------------+----------------------------------------------+
| ia_uri                        | Ignorable                                    |
+-------------------------------+----------------------------------------------+
| schema                        | frame schema info                            |
|                               |                                              |
|                               |  columns: [ (name, type) ]                   |
+-------------------------------+----------------------------------------------+
| row_count                     | number of rows in the frame                  |
+-------------------------------+----------------------------------------------+
| links                         | links to the frame                           |
+-------------------------------+----------------------------------------------+
| entity_type                   | e.g. "frame:", "frame:vertex", "frame:edge"  |
+-------------------------------+----------------------------------------------+
| status                        | status: Active, Deleted, Deleted_Final       |
+-------------------------------+----------------------------------------------+

|

::

   {
    "id": 8,
    "name": "weather_frame1",
    "ia_uri": "ia://frame/8",
    "schema": {
        "columns": []
    },
    "row_count": 0,
    "links": [
        {
            "rel": "self",
            "uri": "http://localhost:9099/v1/frames/8",
            "method": "GET"
        }
    ],
    "entity_type": "frame:",
    "status": "Active"
  }


**Headers** ::

  Content-Length: 279
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
  Server: spray-can/1.3.1
  build_id: TheReneNumber


