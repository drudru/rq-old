#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'fileutils'
require 'fcntl'

def log(mesg)
  print "#{Process.pid} - #{Time.now} - #{mesg}\n"
end

log(Dir.pwd.inspect)

$LOAD_PATH.unshift(File.expand_path("./vendor/gems/json_pure-1.1.6/lib"))
require 'json'

## TEST SECTION

# prep message

rq_port = (ENV['RQ_PORT'] || 3333).to_i

mesg = { 'dest' => "http://127.0.0.1:#{rq_port}/q/test",
         'src'  => 'test',
         'count'  => '2',
         'param1'  => 'done',
         '_method'  => 'prep',
       }

form = { :mesg => mesg.to_json }

# Get the URL
remote_q_uri = "http://127.0.0.1:#{rq_port}/q/test"
res = Net::HTTP.post_form(URI.parse(remote_q_uri + "/new_message"), form)

if res.code != '200'
  print "Sorry, system didn't create test message properly\n"
  print "#{res.inspect}\n"
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
attach_path = File.expand_path("./code/test/fixtures/cornell-box.png")

pipe_res = `curl -0 -s -F filedata=@#{attach_path} -F pathname=cornell-box.png -F msg_id=#{msg_id} -F x_format=json #{remote_q_uri}/#{msg_id}/attach/new`
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

if result[1] != "fd9e598f9eadc9cd045530b11ca9c3bc-Attached successfully"
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

4.times do

  ## Verify that script goes to done state

  remote_q_uri = "http://127.0.0.1:#{rq_port}/q/test/#{msg_id}.json"
  res = Net::HTTP.get_response(URI.parse(remote_q_uri))

  if res.code == '200'
    msg = JSON.parse(res.body)

    if not msg.include?('_attachments') 
      print "Message doesn't contain _attachments field.\n"
      exit 1
    end

    if not msg['_attachments'].include?('cornell-box.png') 
      print "Message doesn't contain attachment cornell-box.png.\n"
      exit 1
    end

    if not msg['_attachments']['cornell-box.png'].include?('md5') 
      print "Message doesn't contain md5 for attachment cornell-box.png.\n"
      exit 1
    end

    if msg['_attachments']['cornell-box.png']['md5'] != 'fd9e598f9eadc9cd045530b11ca9c3bc'
      print "Message doesn't contain correct md5 for attachment cornell-box.png.\n"
      exit 1
    end

    if msg['_attachments']['cornell-box.png']['size'] != 1492960
      print "Message doesn't contain correct file size for attachment cornell-box.png.\n"
      exit 1
    end

    if msg['status'] == 'done - done sleeping'

      # Message in done state, all following tests should be ready

      bad_attach_uri = "http://127.0.0.1:#{rq_port}/q/test/#{msg_id}/Xcornell-box.jpgX"
      res = Net::HTTP.get_response(URI.parse(bad_attach_uri))
      if res.code != '404'
        print "Invalid attach retrieve not responding with 404 code.\n"
        exit 1
      end

      attach_uri = "http://127.0.0.1:#{rq_port}/q/test/#{msg_id}/attach/cornell-box.png"
      res = Net::HTTP.get_response(URI.parse(attach_uri))
      if res.code != '200'
        print "Invalid attach retrieve - request for cornell-box.png should have responded with 200 code.\n"
        exit 1
      end

      if res.body.length != 1492960
        print "Invalid attach retrieve - request for cornell-box.png should have responded with proper body size.\n"
        exit 1
      end

      # TODO: verify md5

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
    
