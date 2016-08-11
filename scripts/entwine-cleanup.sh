#!/usr/bin/env bash
path="${1%/}"

if [ "$2" == "-x" ]; then
    echo -e "Cleaning up...\n"
    (find $path -name "*-[0-9]*" | xargs rm -v)
    echo -e "\n\tAll done - files removed."
else
    echo -e "Affected files:\n"
    (find $path -name "*-[0-9]*")
    echo -e "\n\tNothing actually removed.  Run with -x to delete these files."
fi

