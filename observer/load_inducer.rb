#!/usr/bin/env ruby

require 'observer'

def create_metric(observer, key)
  ts = Time.now
  y = ts.year
  m = ts.month
  d = ts.day       # day of the month
  minute = ts.min  # minute of the day
  hr = ts.hour     # hour of the day
  doy = ts.yday()  # day of the year
  dow = ts.wday()  # day of the week
  woy = ts.strftime('%U') # week of the year
  
  key = {
    'n' => "localdomain",
    'h' => "localhost",
    'o' => "load_inducer",
    'k' => "",
    'y' => y,
    'm' => m,
    'd' => d,
    'hr' => hr
  }

  doc = key.clone
  doc['dy'] = 100
  doc['dw'] = 3
  doc['wy'] = 37
  doc['v'] = {}
  
  for minute in 0..59
    pretty_min = sprintf '%02d', minute
    doc['v'][pretty_min] = 0.0 / 0.0
  end
  
  current_minute = sprintf '%02d', minute
