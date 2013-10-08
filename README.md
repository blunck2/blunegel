Infrastructure Notes

1.) Get and install redis
2.) Start SkyLine
    a.) Run redis as root on aiur with:  ~/redis-2.6.13/src/redis-server ~/skyline/bin/redis.conf
    b.) TBD:  Start Graphite (not necessary but nice to have)
    c.) Start daemons
        i.)   cd ~/skyline/bin
        ii.)  ./horizon.d start
        iii.) ./analyzer.d start
        iv.)  ./webapp.d start
3.) Start MongoDB
    a.) As chris, cd ~/mongodb-linux-x86_64-2.4.5/bin
    b.) ./mongod --dbpath=/mnt/static/db/mongo
4.) Start the MongoDB monitoring agent
    a.) As chris, cd ~/mms-agent
    b.) nohup ./agent.py > nohup.log 2>&1 &
5.)  Start the ruby rest server on aiur
    a.) cd ~/blunegel/observer
    b.) nohup ./rest_server.rb > nohup.out 2>&1 &