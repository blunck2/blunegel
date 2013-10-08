#!/usr/bin/env ruby

require 'sinatra'
require 'json'
require 'mongo'
require 'rubygems'

include Mongo

set :bind, '0.0.0.0'
set :port, 9494

age_off_in_days = 30

client = MongoClient.new('localhost', 27017)
db = client['measurements']
mongo_streams = db['obs.measurements.streams']
mongo_data = db['obs.measurements.data']
mongo_data.create_index([["parsed_date", 1]],
                        :expireAfterSeconds => age_off_in_days * 86400)

puts "connected to mongo.  data retention: #{age_off_in_days} days."

post '/rs/ingest' do
  #mongo_streams.remove
  #mongo_data.remove

  start = Time.now

  records = JSON.parse(request.body.string)

  records.each do |record|
    metadata = record.clone
    metadata.delete("value")
    metadata.delete("timestamp")

    data = record.clone
    data.delete("network")
    data.delete("host")
    data.delete("observer")
    data.delete("key")
    date = Time.at(data["timestamp"])
    data["parsed_date"] = date

    streamid = nil

    key = {"network" => record['network'], 
      "host" => record['host'],
      "observer" => record['observer'],
      "key" => record['key']
    }
    
    h = mongo_streams.update(key, metadata, opts = {:upsert => true })
    cursor = mongo_streams.find(key, opts = {:fields => "_id", :limit => 1})
    streamid = cursor.first()["_id"]
    data["stream"] = streamid

    mongo_data.insert(data)
  end

  stop = Time.now

  delta = stop - start
  rate = records.length / delta

  puts "ingested #{records.length} records in #{delta} seconds (#{rate.to_i} records per second)..."
end

get '/rs' do
  content_type :json
  networks = mongo_streams.distinct(:network)
  return networks.to_json
end

get '/rs/drone/networks/?' do
  content_type :json
  networks = mongo_streams.distinct(:network)
  return networks.to_json
end

get '/rs/drone/:network/?' do
  content_type :json
  hosts = mongo_streams.distinct("host", {"network" => params[:network] })
  return hosts.to_json
end

get '/rs/drone/:network/:host/?' do
  content_type :json
  observers = mongo_streams.distinct("observer", 
                                     { "network" => params[:network],
                                       "host" => params[:host],
                                     })
  return observers.to_json
end

get '/rs/drone/:network/:host/:observer/?' do
  content_type :json
  keys = mongo_streams.distinct("key",
                                { "network" => params[:network],
                                  "host" => params[:host],
                                  "observer" => params[:observer],
                                })
  return keys.to_json
end

get '/rs/drone/:network/:host/:observer/*/?' do
  content_type :json
  metric_key = params[:splat][0]
  query = { "network" => params[:network],
    "host" => params[:host],
    "observer" => params[:observer],
    "key" => metric_key
  }
  cursor = mongo_streams.find(query, opts = {:fields => "_id", :limit => 1})
  r = cursor.first()
  if r != nil
    streamid = r["_id"]
    query = { "stream" => streamid }
    cursor = mongo_data.find(query, 
                             {:fields => { "timestamp" => 1, 
                                           "value" => 1, 
                                           "timestamp" => 1, 
                                           "parsed_date" => 1,
                                           "_id" => 0
                                         }, 
                              :sort => ["timestamp", Mongo::ASCENDING]
                             })

    records = cursor.to_a
    metadata = Hash.new
    metadata["start_timestamp"] = records[0]["timestamp"]
    metadata["parsed_start_timestamp"] = records[0]["parsed_date"]
    metadata["stop_timestamp"] = records[-1]["timestamp"]
    metadata["parsed_stop_timestamp"] = records[-1]["parsed_date"]
    metadata["record_count"] = records.length

    data = records

    results = Hash.new
    results["summary"] = metadata
    results["values"] = data

    return results.to_json
  end

  return Array.new.to_json
end

get '/rs/metric/*/?' do
  content_type :json

  start_time = Time.now

  metric_key = params[:splat][0]
  query = { "key" => metric_key }
  cursor = mongo_streams.find(query, opts = {:fields => ["_id", "network", "host", "observer"]})
  records = cursor.to_a

  summary = Hash.new
  summary["hosts"] = records.length

  values = Hash.new
  host_values = Hash.new

  stream_map = Hash.new
  records.each do |record|
    streamid = record["_id"]
    
    host = record["host"]
    values[host] = Array.new
    host_values[host] = Hash.new
    host_values[host]["values"] = Array.new
    host_values[host]["summary"] = nil
    
    stream_map[record["_id"]] = { "network" => record["network"], 
                                  "host" => record["host"], 
                                  "observer" => record["observer"] }
  end

  #puts "find metrics in these streams: #{streamids}"
  #puts "stream map: #{stream_map}"

  query = { "stream" => { "$in" => stream_map.keys } }
  options = { :sort => ["timestamp", Mongo::ASCENDING] }
  cursor = mongo_data.find(query, options)
  records = cursor.to_a

  #puts "record count: #{records.length}"
  puts "setting records length to: #{records.length}"
  summary["record_count"] = records.length

  puts "record count: #{records.length}"
  

  results = Hash.new
  host_summaries = Hash.new
  records.each do |record|
    value = Hash.new
    timestamp = record["timestamp"]
    parsed_date = record["parsed_date"]
    raw_value = record["value"]

    value["timestamp"] = timestamp
    value["value"] = raw_value
    value["parsed_date"] = parsed_date

    stream_id = record["stream"]
    stream = stream_map[stream_id]
    host = stream["host"]

    host_summary = host_values[host]["summary"]
    if host_summary != nil
      host_summary["record_count"] = host_summary["record_count"] + 1

      if host_summary.has_key?("start_timestamp")
        if timestamp < host_summary["start_timestamp"]
          host_summary["start_timestamp"] = timestamp
          host_summary["parsed_start_timestamp"] = parsed_date
        end
      end

      if host_summary.has_key?("stop_timestamp")
        if timestamp > host_summary["stop_timestamp"]
          host_summary["stop_timestamp"] = timestamp
          host_summary["parsed_stop_timestamp"] = parsed_date
        end
      end

      host_values[host]["summary"] = host_summary



      if timestamp < host_summary["start_timestamp"]
        summary["start_timestamp"] = timestamp
        summary["parsed_start_timestamp"] = parsed_date
      end

      if timestamp > host_summary["stop_timestamp"]
        summary["stop_timestamp"] = timestamp
        summary["parsed_stop_timestamp"] = parsed_date
      end
    else
      initial_summary = Hash.new
      initial_summary["start_timestamp"] = timestamp
      initial_summary["parsed_start_timestamp"] = parsed_date
      initial_summary["stop_timestamp"] = timestamp
      initial_summary["parsed_stop_timestamp"] = parsed_date
      initial_summary["record_count"] = 0

      host_values[host]["summary"] = initial_summary
    end

    host_values[host]["values"].push(value)
  end
  
  results = Hash.new
  results["summary"] = summary
  results["values"] = host_values

  end_time = Time.now

  summary["query_time_in_milliseconds"] = (end_time - start_time) * 1000

  return results.to_json

end


