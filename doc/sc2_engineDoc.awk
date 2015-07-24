#!/bin/awk -f

# This routine is made to add the engine plugin unit to the import statements of a SCALA program
# The file input starts with a comment, a package statement, an import statement, or something else
# We need to ignore any comments and blank lines until something else shows up
# We need to ignore a package statement
# We need to ignore any import statement which sorts lower than our to-be-added statement


BEGIN {
    True = 1;
    False = 0;
    LineAdded = False;
    EngineLine = "import com.intel.taproot.analytics.engine.plugin.";
    Enginez = EngineLine "{Z";
    InsideComment = False;
    CommentAllowed = True;
}

{
    FLine = $0;

    # Only evaluate lines until the engine line is added, then just pass the rest of the lines through unchanged
    if ( LineAdded == False ) {
        # Remove leading white space
        LeadingSpaces = 0;
        LeadingTabs = 0;
        while ( substr(FLine,1,1) == " " || substr(FLine,1,1) == "\t" ) {
            if ( substr(FLine,1,1) == " " ) {
                LeadingSpaces =+ 1;
            } else {
                LeadingTabs =+ 1;
            }
            FLine = substr( FLine, 2, length( FLine ) - 1 );
        }
        if ( length(FLine) > 1 ) {
            if ( InsideComment == False ) {
                if ( substr(FLine,1,2) == "/*" && CommentAllowed == True ) {
                    InsideComment = True;
                } else {
                    if ( substr(FLine,1,2) == "//" ) {
                        # This is a commented Line, ignore it
                    } else {
                        CommentAllowed = False;
                        if ( $1 == "package" ) {
                            # This is a package statement, ignore it
                        } else {
                            if ( $1 == "import" && $0 < Enginez ) {
                                # This is an import statement less than our to-be-added line, ignore it
                            } else {
                                # Add the line now
                                LineAdded = True;
                                if ( $1 == "import" ) {
                                    FLine = EngineLine "Invocation\n" FLine;
                                } else {
                                    FLine = EngineLine "Invocation\n\n" FLine;
                                }
                            }
                        }
                    }
                }
            }
            if ( InsideComment == True ) {
                if ( substr(FLine,1,2) == "*/" ) {
                    InsideComment = False;
                }
            }
        }
        while ( LeadingTabs > 0 ) {
            FLine = "\t" FLine;
            LeadingTabs =- 1;
        }
        while ( LeadingSpaces > 0 ) {
            FLine = " " FLine;
            LeadingSpaces =- 1;
        }
    }
    print FLine;
}
