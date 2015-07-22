#!/bin/awk -f

BEGIN {
    True = 1;
    False = 0;
    LineAdded = False;
    EngineLine = ".engine.plugin.";
}

{
    FLine=$0;
    if ( LineAdded == False ) {
        if ( FLine ~ EngineLine) {
            LineAdded = True;
            if ( FLine ~ /{/) {
                # There is a brace so the methods are inside braces already
                # Just need to add to the existing list
                if ( FLine !~ /ArgDoc/) {
                    FLine = substr( FLine, 1, index( FLine, "{" )) " ArgDoc," substr( FLine, index( FLine, "{") + 1 );
                }
            } else {
                # There is no brace so there is only one method now, presumably Invocation
                # Need to create a set of braces with the methods inside.
                if ( FLine !~ /ArgDoc/) {
                    FLine = substr( FLine, 1, index( FLine, EngineLine ) + length( EngineLine ) - 1 ) "{ ArgDoc, " substr( FLine, index( FLine, EngineLine ) + length( EngineLine ));
                    if ( FLine ~ /\n/ ) {
                        FLine = substr( FLine, 1, index( FLine, "\n" ) - 1 ) " }" substr( FLine, index( FLine, "\n" ));
                    } else {
                        FLine = FLine " }"
                    }
                }
            }
        }
    }
    print FLine;
}
