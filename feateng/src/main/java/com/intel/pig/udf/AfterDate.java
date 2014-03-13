//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.pig.udf;


import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

import java.io.IOException;

public class AfterDate extends FilterFunc {
    @Override
    public Boolean exec(Tuple objects) throws IOException {

        if(objects.size() != 2)
            throw new IOException("Have to pass two DateTime objects");

        DateTime first = (DateTime) objects.get(0);
        DateTime second = (DateTime) objects.get(1);

        return first.isAfter(second);
    }
}
