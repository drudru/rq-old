#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'json'

# Setup a global binding so the GC doesn't close the file
$RQ_IO = IO.for_fd(ENV['RQ_WRITE'].to_i)

# IO tower to RQ mgr process
def write_status(state, mesg = '')
  msg = "#{state} #{mesg}\n"

  STDOUT.write("#{Process.pid} - #{Time.now} - #{msg}")
  $RQ_IO.syswrite(msg)
end

def handle_fail(mesg = 'soft fail')
  count = ENV['RQ_COUNT'].to_i

  if count > 15
    write_status('run', "RQ_COUNT > 15 - failing")
    write_status('fail', "RQ_COUNT > 15 - failing")
    exit(0)
  end

  wait_seconds = count * count * 60
  write_status('resend', "#{wait_seconds}-#{mesg}")
  exit(0)
end

def send_post
  # Construct form post message
  curr_msg = JSON.parse(ENV['RQ_PARAM2'])
  mesg = {}
  keys = %w(src param1 param2 param3 param4 orig_msg_id)
  keys.each do
    |key|
    next unless curr_msg.has_key?(key)
    mesg[key] = curr_msg[key]
  end

  #mesg['_method'] = 'commit'
  uri = ENV['RQ_PARAM1']
  parts = URI.parse(uri).path.split('/')
  parts.pop()  # get rid of new_message
  mesg['dest'] = parts.join('/')


  #mesg['mesg'] = ENV['RQ_PARAM2']


  write_status('run', "Attempting post to url: #{uri}")

  begin
    res = Net::HTTP.post_form(URI.parse(uri), {:x_format => 'json', :mesg => mesg.to_json })
  rescue
    handle_fail("Could not connect to or parse URL: #{uri}")
  end

  if res.code.to_s =~ /2\d\d/
    write_status('done', "successfull post #{res.code.to_s}")
  else
    STDOUT.write("#{Process.pid} - #{Time.now} - #{res.inspect}\n")
    STDOUT.write("#{Process.pid} - #{Time.now} - #{res.body()}\n")
    handle_fail("Could not POST to URL: #{res.inspect}")
  end
end

send_post()
