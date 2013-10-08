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
    * `cd /path/to/mongodb/bin`
    * `./mongod --dbpath ~/exclude/mongo/system/db --port 27017 --logpath ~/exclude/mongo/system/log/mongo.log --fork`
    * `./mongod --dbpath ~/exclude/mongo/data/db --port 27018 --logpath ~/exclude/mongo/data/log/mongo.log --fork`
4. Optionall start the MongoDB monitoring agent
    * As chris, `cd ~/mms-agent`
    * `nohup ./agent.py > nohup.log 2>&1 &`
5.  Start the ruby rest server
    * `sudo gem install sinatra`
    * `sudo gem install json`
    * `sudo gem install mongo`
    * `sudo gem install bson_ext`
    * `cd ~/blunegel/observer`
    * `nohup ./rest_server.rb > nohup.out 2>&1 &`
