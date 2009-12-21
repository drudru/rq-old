#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'fileutils'
require 'fcntl'

def log(mesg)
  print "#{Process.pid} - #{Time.now} - #{mesg}\n"
end


Dir.glob(File.join( "code", "vendor", "gems", "*", "lib")).each do |lib|
  $LOAD_PATH.unshift(File.expand_path(lib))
end
#Dir.glob(File.join("..", "..", "..", "..", "..")).each do |lib|
#  $LOAD_PATH.unshift(File.expand_path(lib))
#end


require 'rubygems'
gem_paths = [File.expand_path(File.join("code", "vendor", "gems")),  Gem.default_dir]
Gem.clear_paths
Gem.send :set_paths, gem_paths.join(":")

#log($LOAD_PATH.inspect)
#log(Dir.pwd.inspect)
#log(gem_paths.inspect)

require 'json'


## TEST SECTION

# prep message

mesg = { 'dest' => 'http://localhost:3333/q/test',
         'src'  => 'test',
         'count'  => '2',
         'param1'  => 'done',
         '_method'  => 'prep',
       }

form = { :x_format => 'json', :mesg => mesg.to_json }

# Get the URL
remote_q_uri = "http://localhost:3333/q/test"
res = Net::HTTP.post_form(URI.parse(remote_q_uri + "/new_message"), form)

if res.code != '200'
  print "Sorry, system didn't create test message properly\n"
  exit 1
end

result = JSON.parse(res.body)

if result[0] != 'ok'
  print "Sorry, system didn't create test message properly : #{res.body}\n"
  exit 1
end

print "Prepped message: #{result[1]}\n"

msg_id = result[1][/\/q\/[^\/]+\/([^\/]+)/, 1]

print "Msg ID: #{msg_id}\n"


# attach message

#form =  :x_format => 'json', '_method' => 'commit', :msg_id => msg_id }
attach_path = File.expand_path("./code/test/fixtures/studio3.jpg")

pipe_res = `curl -s -F filedata=@#{attach_path} -F pathname=studio3.jpg -F msg_id=#{@msg_id} -F x_format=json #{remote_q_uri}/#{msg_id}/attach/new`
#p $?
#p pipe_res
# Get the URL
#res = Net::HTTP.post_form(URI.parse(remote_q_uri + "/#{msg_id}/attach/new"), form)

if $?.exitstatus != 0
  print "Sorry, system couldn't attach to test message properly\n"
  print "Exit status: #{$?.exitstatus.inspect}\n"
  exit 1
end

#if res.code != '200'
#  print "Sorry, system couldn't commit test message properly\n"
#  exit 1
#end

result = JSON.parse(pipe_res)

if result[0] != 'ok'
  print "Sorry, system couldn't attach to test message properly : #{pipe_res}\n"
  exit 1
end

if result[1] != "14a1a7845cc7f981977fbba6a60f0e42-Attached successfully"
  print "Sorry, system couldn't attach to test message properly : #{pipe_res}\n"
  print "Was expecting: #{"14a1a7845cc7f981977fbba6a60f0e42-Attached successfully"}\n"
  exit 1
end

print "Committed message: #{msg_id}\n"


# commit message

form = { :x_format => 'json', '_method' => 'commit', :msg_id => msg_id }

# Get the URL
res = Net::HTTP.post_form(URI.parse(remote_q_uri + "/#{msg_id}"), form)

if res.code != '200'
  print "Sorry, system couldn't commit test message properly\n"
  exit 1
end

result = JSON.parse(res.body)

if result[0] != 'ok'
  print "Sorry, system couldn't commit test message properly : #{res.body}\n"
  exit 1
end

print "Committed message: #{msg_id}\n"


# verify done message

4.times.each do

  ## Verify that script goes to done state

  remote_q_uri = "http://localhost:3333/q/test/#{msg_id}.json"
  res = Net::HTTP.get_response(URI.parse(remote_q_uri))

  if res.code == '200'
    msg = JSON.parse(res.body)

    if not msg.include?('_attachments') 
      print "Message doesn't contain _attachments field.\n"
      exit 1
    end

    if not msg['_attachments'].include?('studio3.jpg') 
      print "Message doesn't contain attachment studio3.jpg.\n"
      exit 1
    end

    if not msg['_attachments']['studio3.jpg'].include?('md5') 
      print "Message doesn't contain md5 for attachment studio3.jpg.\n"
      exit 1
    end

    if msg['_attachments']['studio3.jpg']['md5'] != '14a1a7845cc7f981977fbba6a60f0e42'
      print "Message doesn't contain correct md5 for attachment studio3.jpg.\n"
      exit 1
    end

    if msg['status'] == 'done - done sleeping'
        print "Message went into proper state. ALL DONE\n"
        exit 0
    end
  end

  #print "-=-=-=-\n"
  #print res.code
  #print res.body
  #print "\n"
  #print "-=-=-=-\n"

  sleep 1
end



print "FAIL - system didn't get a message in proper state: #{res.body}\n"
exit 1
    
