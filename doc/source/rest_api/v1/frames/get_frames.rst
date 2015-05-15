===============
Frames REST API
===============

----------------
Get Named Frames
----------------

GET /v1/frames/
===============

Request
-------

**Route** ::

  GET /v1/frames/

**Body**

(None)


**Headers** ::

  Authorization: test_api_key_1
  Content-type: application/json

Response
--------

**Status** ::

  200 OK

**Body**

Returns a list of frame entity entries, where an entry is defined as...

+-------------------------------+----------------------------------------------+
| Name                          | Description                                  |
+===============================+==============================================+
| id                            | frame id (engine-assigned)                   |
+-------------------------------+----------------------------------------------+
| name                          | frame name (user-assigned)                   |
+-------------------------------+----------------------------------------------+
| url                           | url to the frame                             |
+-------------------------------+----------------------------------------------+
| entity_type                   | e.g. "frame:", "frame:vertex", "frame:edge"  |
+-------------------------------+----------------------------------------------+

|

::

  Example:
  [
    {
        "id": 7,
        "name": "super_frame",
        "url": "http://localhost:9099/v1/frames/7",
        "entity_type": "frame:"
    },
    {
        "id": 8,
        "name": "weather_frame1",
        "url": "http://localhost:9099/v1/frames/8",
        "entity_type": "frame:"
    }
  ]


Headers::

  Content-Length: 279
  Content-Type: application/json; charset=UTF-8
  Date: Thu, 14 May 2015 23:42:27 GMT
  Server: spray-can/1.3.1
  build_id: TheReneNumber


