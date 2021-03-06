require 'rubygems'
require 'mongo'
require 'date'

include Mongo


DEFAULT_SYSTEM_HOSTNAME = "localhost"
DEFAULT_SYSTEM_PORT = 27017
DEFAULT_SYSTEM_DATABASE = "streams"

DEFAULT_DATA_HOSTNAME = "localhost"
DEFAULT_DATA_PORT = 27018
DEFAULT_DATA_DATABASE = "measurements"


class Client  
  class << self
    def system()
      new(DEFAULT_SYSTEM_HOSTNAME, DEFAULT_SYSTEM_PORT, DEFAULT_SYSTEM_DATABASE)
    end
    def data()
      new(DEFAULT_DATA_HOSTNAME, DEFAULT_DATA_PORT, DEFAULT_DATA_DATABASE)
    end
    
    private :new
  end
  
  def client
    @client
  end

  def db
    @db
  end

  def initialize(hostname, port, database)
    @client = MongoClient.new(hostname, port)
    @db = @client[database]
  end

end


class Ingester
  MINUTE_LEVEL_AGE_OFF_IN_DAYS = 30  # 1 month @ 1 minute granularity
  HOUR_LEVEL_AGE_OFF_IN_DAYS = 180   # 6 months @ 1 hour granularity
  DAY_LEVEL_AGE_OFF_IN_DAYS = 3650   # 10 years @ 1 day granularity

  def initialize(streams, data)
    @mongo_streams = streams.db['streams']
    @mongo_data_minute = data.db['minute']
    @mongo_data_hour = data.db['hour']
    @mongo_data_day = data.db['day']

    @mongo_data_minute.create_index([["b", 1]], :expireAfterSeconds => MINUTE_LEVEL_AGE_OFF_IN_DAYS * 86400)
    @mongo_data_minute.create_index([["n", 1], ["h", 1], ["o", 1], ["k", 1], ["y", 1], ["m", 1], ["d", 1], ["hr", 1]])
    @mongo_data_hour.create_index([["b", 1]], :expireAfterSeconds => HOUR_LEVEL_AGE_OFF_IN_DAYS * 86400)
    @mongo_data_hour.create_index([["n", 1], ["h", 1], ["o", 1], ["k", 1], ["y", 1], ["m", 1], ["d", 1]])
    @mongo_data_day.create_index([["b", 1]], :expireAfterSeconds => DAY_LEVEL_AGE_OFF_IN_DAYS * 86400)
    @mongo_data_day.create_index([["n", 1], ["h", 1], ["o", 1], ["k", 1], ["y", 1], ["m", 1]])

    puts "connected to mongo."
    puts "data retention policy:"
    puts "  * 1 minute granularity: #{MINUTE_LEVEL_AGE_OFF_IN_DAYS} days"
    puts "  * 1 hour granularity:   #{HOUR_LEVEL_AGE_OFF_IN_DAYS} days"
    puts "  * 1 day granularity:    #{DAY_LEVEL_AGE_OFF_IN_DAYS} days"
  end

  def ingest(records)
    start = Time.now
    
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
    
    @mongo_streams.update(key, doc, opts = {:upsert => true })
  end

  def insert_data(record)
    insert_minute(record)
    insert_hour(record)
    insert_day(record)
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

    cursor = @mongo_data_minute.find(key, opts = {:limit => 1})
    e = cursor.first()
    if e.nil?
      doc = key.clone
      doc['dy'] = doy
      doc['dw'] = dow
      doc['wy'] = woy
      doc['v'] = {}
      for minute in 0..59
        pretty_min = sprintf '%02d', minute
        doc['v'][pretty_min] = 0.0 / 0.0
      end
      
      current_minute = sprintf '%02d', minute
      doc['v'][current_minute] = value
      
      #puts "minute insert: #{y}-#{m}-#{d} #{hr}:#{current_minute} => #{value}"
      @mongo_data_minute.insert(doc)
    else
      #puts "minute update: #{y}-#{m}-#{d} #{hr}:#{minute} => #{value}"
      min = sprintf "%02d", minute
      e['v'][min] = value
      @mongo_data_minute.update(key, e)
    end
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
    
    cursor = @mongo_data_hour.find(key, opts = {:limit => 1})
    e = cursor.first()
    if e.nil?
      doc = key.clone
      doc['dy'] = doy
      doc['dw'] = dow
      doc['wy'] = woy
      doc['v'] = {}
      for hour in 0..23
        pretty_hour = sprintf '%02d', hour
        doc['v'][pretty_hour + "t"] = 0.0
        doc['v'][pretty_hour + "c"] = 0.0
        doc['v'][pretty_hour + "a"] = 0.0
        doc['v'][pretty_hour + "z"] = 0.0
      end
      
      current_hour = sprintf '%02d', hr
      doc['v'][current_hour + "t"] = value
      doc['v'][current_hour + "c"] = 1
      doc['v'][current_hour + "a"] = value
      doc['v'][current_hour + "z"] = value
      
      #puts "hour insert: #{y}-#{m} #{hr} #{h}:/#{k} => #{value}"
      @mongo_data_hour.insert(doc)
    else
      #puts "hour update: #{y}-#{m}-#{d} #{hr} #{h}:/#{k} => #{value}"
      hr = sprintf '%02d', hr
      #PP.pp(e)
      if value < e["v"][hr + "a"]
        e["v"][hr + "a"] = value
      end
      if value > e["v"][hr + "z"]
        e["v"][hr + "z"] = value
      end
      
      e["v"][hr + "c"] =  e["v"][hr + "c"] + 1
      e["v"][hr + "t"] =  e["v"][hr + "t"] + value
      
      @mongo_data_hour.update(key, e)
    end
  end
  
  
  def insert_day(record)
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
    }
    
    
    #puts "looking for: (n=#{n}, h=#{h}, o=#{o}, k=#{k}, y=#{y})"
    
    cursor = @mongo_data_day.find(key, opts = {:limit => 1})
    e = cursor.first()
    if e.nil?
      doc = key.clone
      
      doc['v'] = {}
      for day in 1..days_in_month(y, m)
        pretty_day = sprintf '%02d', day
        doc['v'][pretty_day + "t"] = 0.0
        doc['v'][pretty_day + "c"] = 0.0
        doc['v'][pretty_day + "a"] = 0.0
        doc['v'][pretty_day + "z"] = 0.0
      end
      
      current_day = sprintf '%02d', d
      doc['v'][current_day + "t"] = value
      doc['v'][current_day + "c"] = 1
      doc['v'][current_day + "a"] = value
      doc['v'][current_day + "z"] = value
      
      #puts "day insert: #{y}-#{m}-#{d}  #{h}:/#{k} => #{value}"
      @mongo_data_day.insert(doc)
    else
      #puts "day update: #{y}-#{m}-#{d}  #{h}:/#{k} => #{value}"
      dom = sprintf '%02d', d
      #PP.pp(e)
      if value < e["v"][dom + "a"]
        e["v"][dom + "a"] = value
      end
      if value > e["v"][dom + "z"]
        e["v"][dom + "z"] = value
      end
      
      e["v"][dom + "c"] =  e["v"][dom + "c"] + 1
      e["v"][dom + "t"] =  e["v"][dom + "t"] + value
      
      #puts "updating with: #{e}"
      @mongo_data_day.update(key, e)
    end
  end
  
  def days_in_month(year, month)
    (Date.new(year, 12, 31) << (12-month)).day
  end
  
end
