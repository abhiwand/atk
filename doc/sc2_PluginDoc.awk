#!/bin/awk -f

BEGIN {
    LineAdded = 1;
    EngineLine = ".engine.plugin.";
}

{
    FLine=$0;
    if ( LineAdded == 1 ) {
        if ( FLine ~ EngineLine ) {
            LineAdded = 0;
            if ( FLine ~ /{/) {
                # There is a brace so the methods are inside braces already
                # Just need to add to the existing list
                if ( FLine !~ /PluginDoc/) {
                    FLine = substr( FLine, 1, index( FLine, "{" )) " PluginDoc," substr( FLine, index( FLine, "{") + 1 );
                }
            } else {
                # There is no brace so there is only one method now, presumably Invocation
                # Need to create a set of braces with the methods inside.
                FLine = substr( FLine, 1, index( FLine, EngineLine ) + length( EngineLine ) - 1 ) "{ PluginDoc, " substr( FLine, index( FLine, EngineLine ) + length( EngineLine ));
                if ( FLine ~ /\n/ ) {
                    FLine = substr( FLine, 1, index( FLine, "\n" ) - 1 ) " }" substr( FLine, index( FLine, "\n" ));
                } else {
                    FLine = Fline " }"
                }
            }
        }
    }
    print FLine;
}
