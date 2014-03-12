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

