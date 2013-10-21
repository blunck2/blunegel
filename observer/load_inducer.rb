#!/usr/bin/env ruby

require './observer'
require 'pp'
require './ingest'


def generate_metric(network, hostname, observer, key)
  ts = Time.now
  
  metric = {
    'network' => network,
    'host' => hostname,
    'observer' => observer,
    'key' => key,
    'timestamp' => ts.to_i,
    'value' => rand(100)
  }

  return metric
end

def generate_observer(network, hostname, observer, key_count)
  metrics = []
  (0..key_count).each do |key_number|
    key = 'key_' << "%02d" % key_number
    metrics << generate_metric(network, hostname, observer, key)
  end

  return metrics
end

def generate_host(network, hostname, observer_count, keys)
  observers = []
  (0..observer_count).each do |observer_number|
    observer = 'observer_' << '%02d' % observer_number
    observers.concat(generate_observer(network, hostname, observer, keys))
  end

  return observers
end

def generate_network(network, host_count, observers, keys)
  hosts = []
  (0..host_count).each do |host_number|
    hostname = 'host_' << "%04d" % host_number
    hosts.concat(generate_host(network, hostname, observers, keys))
  end

  return hosts
end


network = ARGV[0]
hosts = ARGV[1].to_i
observers = ARGV[2].to_i
keys = ARGV[3].to_i

puts "generating metrics for: #{network}"

records = generate_network(network, hosts, observers, keys)
PP.pp(records)


puts "ingesting..."
ingester = Ingester.new("load_test")
ingester.ingest(records)
puts "done."


