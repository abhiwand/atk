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

from intelanalytics import *

#loggers.set_http()

print("server ping")
server.ping()

print("define csv file")
schema =  [("number", int32), ("factor", str), ("binary", str), ("isPrime", str), ("reverse", str), ("isPalindrome", str)]
csv = CsvFile("/numbers.csv", schema, delimiter=":", skip_header_lines=1)

print("create big frame")
frame = BigFrame(csv)

print("inspect frame")
print frame.inspect(30)
print("frame row count " + str(frame.row_count))

print("bin column")
binned = frame.bin_column('number', 4, 'equalwidth', 'number_binned')
print("inspect frame")
print binned.inspect(30)
