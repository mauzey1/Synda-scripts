#!/bin/bash

# standard CMIP6 installs: everything of mon or slower frequency for all nodes,
# also everything of day or 6hr frequency (not 6hrPlev) for selected nodes,
# and all the "high priority" CMIP6 data.

export LOGFILE=/var/log/synda/install/install-`date -d sunday --iso-8601=date`.log
#...was export LOGFILE=/var/log/synda/install/install.log
echo >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` "begin standard_installs_v3.sh" >> $LOGFILE 2>&1
#includes UTC offset, not allowed in Synda: export TODATE=`date --iso-8601=seconds`Z
export TODATE=`date +%Y-%m-%dT%H:%M:%SZ`

# yr and high priority, high frequency
echo `date --iso-8601=minutes` 'incr yrfx' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-yrfx-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-yrfx-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri 3hr' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-3hr-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-3hr-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri CF3hr' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-CF3hr-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-CF3hr-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri E3hr' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-E3hr-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-E3hr-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri AERday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-AERday-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-AERday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri CFday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-CFday-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-CFday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri Eday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-Eday-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-Eday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr pri Oday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-Oday-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6priority-Oday-http-2023.08.02.txt >> $LOGFILE 2>&1
# end of high priority high frequency

# monthly data
echo `date --iso-8601=minutes` 'incr monfx gridftp' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-monfx-gridftp-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http Amon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Amon-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http CFmon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-CFmon-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http Emon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Emon-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http Lmon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Lmon-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http Omon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Omon-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr mon http othermon' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-othermon-http-2023.08.02.txt >> $LOGFILE 2>&1
# end of monthly data

# daily data.  First "best" nodes, gridftp and http, then all nodes believed to support gridftp,
# then all nodes, one table at a time
echo `date --iso-8601=minutes` 'incr day, best data nodes' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-day-bestnodes-gridftp-2023.08.02.txt>> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-day-bestnodes-http-2023.08.02.txt>> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'incr day, all data nodes' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-day-gridftp-2023.08.02.txt>> $LOGFILE 2>&1

echo `date --iso-8601=minutes` 'extra- AERday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-AERday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- CFday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-CFday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- Eday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Eday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- EdayZ' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-EdayZ-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- Oday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-Oday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- SIday' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-SIday-http-2023.08.02.txt >> $LOGFILE 2>&1
echo `date --iso-8601=minutes` 'extra- day' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-day-http-2023.08.02.txt >> $LOGFILE 2>&1
# end of day installations

# 6hr data from better-performing data nodes
echo `date --iso-8601=minutes` 'incr 6hr, best data nodes' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-6hr-bestnodes-gridftp-2023.08.02.txt>> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CMIP6-6hr-bestnodes-http-2023.08.02.txt>> $LOGFILE 2>&1

# CREATE-IP
echo `date --iso-8601=minutes` 'CREATE-IP (reanalysis)' >> $LOGFILE 2>&1
echo y | synda install -i --timestamp_right_boundary $TODATE -s ~/selection_files/CREATE-IP_all_2023.08.02.txt>> $LOGFILE 2>&1

# Mark 'waiting' or 'error' files as 'obsolete' if a newer version of them exists
# This prevents Synda from downloading them
echo `date --iso-8601=minutes` "marking obsolete files" >> $LOGFILE 2>&1
sqlite3 /var/lib/synda/sdt/sdt.db ".read /home/syndausr/scripts/obsolete.sql" >> $LOGFILE 2>&

# End
echo `date --iso-8601=minutes` "end standard_installs_v3.sh" >> $LOGFILE 2>&1
echo >> $LOGFILE 2>&1


