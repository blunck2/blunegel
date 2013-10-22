#!/usr/bin/env ruby

require 'rubygems'
require 'sinatra'
require 'json'
require 'pp'

require './ingest'

set :bind, '0.0.0.0'
set :port, 9494

ingester = Ingester.new(Client.system(), Client.data())
post '/rs/ingest' do
  records = JSON.parse(request.body.string)
  #PP.pp(records)
  ingester.ingest(records)
end

