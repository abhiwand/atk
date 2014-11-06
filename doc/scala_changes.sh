#! /bin/sh

# This will check for any changes which have occured to the scala files.
# It should be run after 'git' updating the main branch of code.

# This is what we look for
s=CommandDoc\(

# First make sure the comparison directory exists. The data files will go here.
w=.CommandDoc
if [ ! -d $w ]; then mkdir $w; fi

# Now create a (hopefully) new file name and make it accessable as a variable
d=$w/$(date +%Y%m%d-%H%M).lst
echo "echo $d" > $w/set_path.sh
chmod +x $w/set_path.sh

# If the file already exists, then it is too soon to check again.
if [ ! -e $d ]; then

    # Get the name of the most recent listing file.
    f=""
    for g in $w/*.lst
    do
        f=$g
    done

    # Create a new listing file with the date of any scala file with python docstrings.
    echo "Current Listing;" > $d
    for g in $(grep -ril --include=*.scala --exclude=CommandDoc.scala $s ../)
        do
            echo $(date -r $g) $g >> $d
        done

    # Make sure there is a file to compare to
    if [ "$f" != "" ]; then

        # If the current listing is different than the previous listing, we need to deal with it
        diff $d $f > /dev/null
        if [ "$?" = "1" ]; then

            # Show the differences between the current listing and the previous one.
            vimdiff $d $f

        fi
    fi
fi
