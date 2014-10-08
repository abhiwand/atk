#! /bin/sh

# This will check for any changes which have occured to the scala files.
# It should be run after 'git' updating the main branch of code.

# This is what we look for
s=CommandDoc\(oneLineSummary

# First make sure the comparison directory exists. The data files will go here.
w=~/commanddoc
if test ! -d $w; then mkdir $w; fi

# Now create a (hopefully) new file name
d=$w/$(date +%Y%m%d-%H%M).lst
echo "echo $d" > scala_changes
chmod +x scala_changes

# If the file already exists, then it is too soon to check again.
if test ! -e $d; then

    # Get the name of the most recent listing file.
    ls -1 $w/*.lst 2>/dev/null > $d
    f=$(tail -n 1 $d)

    # Create a new listing file with the date of any scala file with python docstrings.
    echo "Current Listing;" > $d
    for g in $(grep -ril --include=*.scala $s ../)
        do
            echo $(date -r $g) $g >> $d
        done

    # Make sure there is a file to compare to
    if test "$f" != ""; then

        # If the current listing is different than the previous listing, we need to deal with it
        diff $d $f > /dev/null
        if test "$?" = "1"; then

            # Show the differences between the current listing and the previous one.
            vimdiff $d $f

        fi
    fi
fi
