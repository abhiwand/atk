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

package com.intel.event;


import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.regex.Pattern;

public class RegularExpressionMatcher extends TypeSafeMatcher {

    private final Pattern pattern;

    public RegularExpressionMatcher(String pattern) {
        this(Pattern.compile(pattern));
    }

    public RegularExpressionMatcher(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matches regular expression ").appendValue(pattern);
    }

    public boolean matchesSafely(String item) {
        return pattern.matcher(item).matches();
    }


    @Factory
    public static Matcher matchesPattern(Pattern pattern) {
        return new RegularExpressionMatcher(pattern);
    }

    @Factory
    public static Matcher matchesPattern(String pattern) {
        return new RegularExpressionMatcher(pattern);
    }

    @Override
    protected boolean matchesSafely(Object o) {
        return matchesSafely(String.valueOf(o));
    }
}
