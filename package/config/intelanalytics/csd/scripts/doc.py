//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

import tornado.ioloop
import tornado.web
import argparse

parser = argparse.ArgumentParser(description="Get CDH configurations for ATK")
parser.add_argument("--host", type=str, help="Documentation Server Host address", default="0.0.0.0")
parser.add_argument("--port", type=int, help="Documentation Server Port ", default=80)
parser.add_argument("--path", type=str, help="Documentation path", default="admin")

args = parser.parse_args()

settings = {'debug': True,
            'static_path': args.path}

application = tornado.web.Application([
    (r"/", tornado.web.StaticFileHandler),
    ],**settings)

if __name__ == "__main__":
    application.listen(args.port, args.host)
    tornado.ioloop.IOLoop.instance().start()
