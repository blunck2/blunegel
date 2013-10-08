Infrastructure Notes
====================

1. Get and install redis
2. Start SkyLine
    * Run redis as root on aiur with:
       
 `~/redis-2.6.13/src/redis-server`
       
 `~/skyline/bin/redis.conf`
 
   * TBD:  Start Graphite (not necessary but nice to have)

   * Start daemons

 `cd ~/skyline/bin`

 `./horizon.d start`

 `./analyzer.d start`

 `./webapp.d start`

3. Start MongoDB
    * As chris, `cd ~/mongodb-linux-x86_64-2.4.5/bin`
    * `./mongod --dbpath=/mnt/static/db/mongo`
4. Start the MongoDB monitoring agent
    * As chris, `cd ~/mms-agent`
    * `nohup ./agent.py > nohup.log 2>&1 &`
5.  Start the ruby rest server on aiur
    * `cd ~/blunegel/observer`
    * `nohup ./rest_server.rb > nohup.out 2>&1 &`
