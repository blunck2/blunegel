#!/usr/bin/env ruby

require 'rubygems'
require 'sinatra'
require 'json'
require 'mongo'
require 'pp'

include Mongo

set :bind, '0.0.0.0'
set :port, 9494

minute_level_age_off_in_days = 30  # 1 month @ 1 minute granularity
hour_level_age_off_in_days = 180   # 6 months @ 1 hour granularity
day_level_age_off_in_days = 3650   # 10 years @ 1 day granularity

# system db information
system_client = MongoClient.new('localhost', 27017)
system_db = system_client['application']
$mongo_streams = system_db['streams']

# data db information
data_client = MongoClient.new('localhost', 27018)
data_db = data_client['data']
$mongo_data_minute = data_db['minute']
$mongo_data_hour = data_db['hour']
$mongo_data_day = data_db['day']

$mongo_data_minute.create_index([["b", 1]], :expireAfterSeconds => minute_level_age_off_in_days * 86400)
$mongo_data_hour.create_index([["b", 1]], :expireAfterSeconds => hour_level_age_off_in_days * 86400)
$mongo_data_day.create_index([["b", 1]], :expireAfterSeconds => day_level_age_off_in_days * 86400)

puts "connected to mongo."
puts "data retention policy:"
puts "  * 1 minute granularity: #{minute_level_age_off_in_days} days"
puts "  * 1 hour granularity:   #{hour_level_age_off_in_days} days"
puts "  * 1 day granularity:    #{day_level_age_off_in_days} days"

post '/rs/ingest' do
  #mongo_system.remove
  #mongo_data_minute.remove
  #mongo_data_hour.remove
  #mongo_data_day.remove

  start = Time.now

  records = JSON.parse(request.body.string)

  records.each do |record|
    insert_stream(record)
    insert_data(record)
  end

  stop = Time.now

  delta = stop - start
  rate = records.length / delta

  puts "ingested #{records.length} records in #{delta} seconds (#{rate.to_i} records per second)..."
end


def insert_stream(record)
  r = record.clone
  r.delete("value")
  
  key = {
    "n" => r['network'], 
    "h" => r['host'],
    "o" => r['observer'],
    "k" => r['key'],
  }
  
  doc = key.clone
  doc["sd"] = Time.at(r["timestamp"])  # send date (time record was sent to us)
  doc["rd"] = Time.now                 # receive date (time we received the record)
    
  $mongo_streams.update(key, doc, opts = {:upsert => true })
end

def insert_data(record)
  insert_minute(record)
  insert_hour(record)
  #insert_day(record)
end

def insert_minute(record)
  r = record.clone
  raw = r["timestamp"]
  value = r['value']

  n = r['network']
  h = r['host']
  o = r['observer']
  k = r['key']
  v = r['value']

  ts = Time.at(r['timestamp'])
  y = ts.year
  m = ts.month
  d = ts.day       # day of the month
  minute = ts.min  # minute of the day
  hr = ts.hour     # hour of the day
  doy = ts.yday()  # day of the year
  dow = ts.wday()  # day of the week
  woy = ts.strftime('%U') # week of the year

  key = { 
    'n' => n,
    'h' => h,
    'o' => o,
    'k' => k,
    'y' => y,
    'm' => m,
    'd' => d,
    'hr' => hr
  }


  #puts "looking for: (n=#{n}, h=#{h}, o=#{o}, k=#{k}, y=#{y}, m=#{m}, d=#{d}, hr=#{hr})"

  cursor = $mongo_data_minute.find(key, opts = {:limit => 1})
  e = cursor.first()
  if e.nil?
    doc = key.clone
    doc['dy'] = doy
    doc['dw'] = dow
    doc['wy'] = woy
    doc['v'] = {}
    for minute in 0..59
      pretty_min = sprintf '%02d', minute
      doc['v'][pretty_min] = "NaN".to_f
    end
    
    current_minute = sprintf '%02d', minute
    doc['v'][current_minute] = value
    
    puts "minute insert: #{y}-#{m}-#{d} #{hr}:#{current_minute} => #{value}"
    $mongo_data_minute.insert(doc)
  else
    puts "minute update: #{y}-#{m}-#{d} #{hr}:#{minute} => #{value}"
    e['v'][minute.to_s] = value
    $mongo_data_minute.update(key, e)
  end
  
  #mongo_data.insert(data)

end

def insert_hour(record)
  r = record.clone
  raw = r["timestamp"]
  value = r['value']

  n = r['network']
  h = r['host']
  o = r['observer']
  k = r['key']
  v = r['value']

  ts = Time.at(r['timestamp'])
  y = ts.year
  m = ts.month
  d = ts.day       # day of the month
  hr = ts.hour     # hour of the day
  doy = ts.yday()  # day of the year
  dow = ts.wday()  # day of the week
  woy = ts.strftime('%U') # week of the year

  key = { 
    'n' => n,
    'h' => h,
    'o' => o,
    'k' => k,
    'y' => y,
    'm' => m,
    'd' => d,
  }


  #puts "looking for: (n=#{n}, h=#{h}, o=#{o}, k=#{k}, y=#{y}, m=#{m}, d=#{d})"

  cursor = $mongo_data_hour.find(key, opts = {:limit => 1})
  e = cursor.first()
  if e.nil?
    doc = key.clone
    doc['dy'] = doy
    doc['dw'] = dow
    doc['wy'] = woy
    doc['v'] = {}
    for hour in 0..23
      pretty_hour = sprintf '%02d', hour
      doc['v'][pretty_hour] = "NaN".to_f
      doc['v'][pretty_hour + "t"] = "NaN".to_f
      doc['v'][pretty_hour + "v"] = "NaN".to_f
      doc['v'][pretty_hour + "a"] = "NaN".to_f
      doc['v'][pretty_hour + "z"] = "NaN".to_f
    end
    
    current_day = sprintf '%03d', doy
    doc['v'][current_day + "t"] = 0
    doc['v'][current_day + "v"] = value
    doc['v'][current_day + "a"] = value
    doc['v'][current_day + "z"] = value
    
    puts "hour insert: #{y}-#{m} #{hr} => #{value}"
    $mongo_data_hour.insert(doc)
  else
    puts "#{y}"
    puts "#{m}"
    puts "#{d}"
    puts "#{hr}"
    puts "#{value}"

    puts "hour update: #{y}-#{m}-#{d} #{hr}: => #{value}"
    hr = sprintf '%02d', hr
    PP.pp(e)
    if value < e["v"][hr + "a"]
      e[hr + "a"] = value
    end
    if value > e["v"][hr + "z"]
      e[hr + "z"] = value
    end

    if e["v"][hr + "c"] == nil
      e["v"][hr + "c"] = value
    else
      e["v"][hr + "c"] =  e["v"][hr + "c"] + 1
      e["v"][hr + "t"] =  e["v"][hr + "t"] + value
    end

    $mongo_data_hour.update(key, e)
  end
  
  #mongo_data.insert(data)

end


