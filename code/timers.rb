
#
# Run
#
# Watch directory every so often
#
# Create and down queues as necessary
#


require 'fileutils'
require 'json'
require 'code/simplepc'
require 'code/unixsocket'
require 'code/queueclient'


module RQ

  class TimerRecord < Struct.new(
    :name,          # name
    :config_stat,   # File.stat
    :config,        # JSON converted to Ruby objects
    :due            # due in epoch seconds
  )
  end

  class Timers

    def initialize(child_rd)
      @timers = {}
      @due = 0
      @parent_rd_pipe = child_rd    # read-only end of pipe from parent

      @sent = 0
      @errors = 0
    end

    def run!

      # The timer process keeps track of timers in memory
      #
      # ROUGH NOTES
      # - timer process - sends periodic messages
      # - no assumption about timezones etc.
      # - no assumptions about previous runs
      # * implication - up to receiver to handle too many messages !
      # - scans directory every 60 seconds
      # - syncs in memory with json files in directory
      # - dest queue, period (in minutes), message template
      # - when it is time, it injects into relay, simple linear scan on
      #   list sorted by due time
      # - it has a log
      # - web shows list of entries, due in x seconds, and period,
      # - has a 'send now' method
      #
      # If a new timer, create a timer
      # If a json changed, reload, but only alter due_time if period changes
      # If a running timer is no longer in 'timers', shutdown a timer
      #
      # In the future, we would only run this if the directory changed
      # or the json file changes


      # Wait for queue mgr to start things
      # TODO: why not ask queuemgr for state instead
      sleep 3
      $log.info("rq-timer running")

      @unixsock = RQUNIXSocket.start_server('timers/sock')

      poll_time = 60

      reqs = {}

      while true

        @now = Time.now.to_i
        poll_time = @due - @now
        if (poll_time <= 0)
          poll_time = 60
          @due = @now + poll_time
        end

        scan_dir()

        ready = check_time_for_ready()

        send_messages(ready)

        set_new_due(ready)

        # sockets returns [ read_sock, write_sock ]
        # nil is allowed
        reqs_socks = reqs.map { |k,v| v.sockets }

        if reqs_socks.empty?
          reqs_rd, reqs_wr = [ [], [] ]
        else
          reqs_rd, reqs_wr = reqs_socks.transpose
        end
        reads = [@unixsock, @parent_rd_pipe] + reqs_rd.compact
        writes = reqs_wr.compact

        rdy_rd, rdy_wr, _ = IO.select(reads, writes, nil, poll_time)

        if rdy_rd.nil?  # a timeout occurred
          rdy_rd = []
          rdy_wr = []
        end

        rdy_rd.each do |io|
          if io == @unixsock
            client_socket, client_sockaddr = RQUNIXSocket.do_accept(@unixsock)
            next unless client_socket

            req = SimplePC::RecvPacket.new(client_socket)
            reqs[client_socket] = req
          elsif io == @parent_rd_pipe
            # Parent is only readable when closing
            $log.info("noticed parent close exiting...")
            shutdown!
          else
            req = reqs[io]
            req.process_io()

            # if still in process, we do nothing
            if req.state == :done
              reply = handle_recv(req)
              #reqs.delete(req.sock) - same socket
              reqs[reply.sock] = reply
              rdy_wr << reply.sock
              # It is ready to write, so let the loop below attempt
              # to write it to the socket
            elsif req.state == :timeout
              $log.info("client timeout. closing...")
              reqs.delete(req.sock)
            elsif req.state == :err
              $log.info("client error. closing...")
              reqs.delete(req.sock)
            end

          end
        end
        rdy_wr.each do |io|
          req = reqs[io]
          req.process_io()

          # if still in :send, we do nothing
          if req.state == :done
            reqs.delete(req.sock)
            io.close
          elsif req.state == :timeout
            $log.info("reply client timeout. closing...")
            reqs.delete(req.sock)
            io.close
          elsif req.state == :err
            $log.info("reply client error. closing...")
            reqs.delete(req.sock)
            io.close
          end
        end

        deletes = reqs.select { |sock,req| req.timed_out? }
        deletes.each {
          |req|
          $log.info("client timed out. closing...")
          reqs.delete(req.sock)
          req.sock.close
        }
      end
    end

    private

    def handle_recv(req)
      if req.cmd == 'get_timers'
        result = @timers.map { |k,trec| [k, trec.due] }
        SimplePC::SendPacket.send(req.sock, 'ok', result)
      elsif req.cmd == 'get_status'
        result = { "sent" => @sent, "errors" => @errors }
        SimplePC::SendPacket.send(req.sock, 'ok', result)
      elsif req.cmd == 'zzz'
      end
    end

    def scan_dir()
      # Get list of valid timers by scanning directory called 'timers'
      names = Dir.entries('timers').select do |ent|
        ent.end_with?('.json') &&
          ent.count('.') == 1 &&   # only one period
          File.readable?(File.join('timers', ent)) &&
          File.symlink?(File.join('timers', ent))
      end

      names = names.map { |e| e.split('.')[0] }  # just take the name

      # This will replace @timers
      new_timers = {}

      names.each {
        |name|

        timer_rec = @timers[name]
        if timer_rec
          stat = File.stat("timers/#{name}.json") rescue nil
          if timer_rec.config_stat == stat
            new_timers[name] = timer_rec
          else
            config = get_config(name)
            if config
              timer_rec.config = config
              timer_rec.due = @now
              timer_rec.config_stat = stat
              new_timers[name] = timer_rec
            else
              # We put an error in the log via 'get_config', we could just
              # put the existing back in, but that would violate
              # a principle. Fail fast and never hide errors
              # By not adding it, it is as if the symlink was removed
              @errors += 1
              $log.error("error config issue #{name}")
            end
          end
        else
          timer_rec = TimerRecord.new

          config = get_config(name)
          if config
            timer_rec.name = name
            timer_rec.config = config
            timer_rec.due = @now
            timer_rec.config_stat = stat
            new_timers[name] = timer_rec
          end
        end
      }

      # Old timers don't need to have any action performed
      # when they are deleted, so their data just gets
      # gc'd

      @timers = new_timers
    end

    def check_time_for_ready()
      # Check to see if any timers are ready to fire

      @timers.select { |k,trec| trec.due <= @now }
    end

    def send_messages(rdy)
      rdy.each do
        |k,trec|

        begin
          if trec.config['msg']['dest'].start_with?('http:')
            que_name = 'relay'
          else
            que_name = trec.config['msg']['dest']
          end

          qc = RQ::QueueClient.new(que_name)

          # Construct message
          mesg = {}
          keys = %w(dest param1 param2 param3 param4)
          keys.each do |key|
            next unless trec.config['msg'].has_key?(key)
            mesg[key] = trec.config['msg'][key]
          end
          result = qc.create_message(mesg)
          print "#{result[0]} #{result[1]}\n"
          if result[0] == "ok"
            @sent += 1
          else
            @errors += 1
            $log.error("error when sending message #{trec.name}")
            $log.error("bad result for create_message #{result.inspect}")
          end
        rescue
          @errors += 1
          $log.error("exception when sending message #{trec.name}")
          $log.error($!)
        end
      end
    end

    def set_new_due(rdy)
      rdy.each { |k,trec| trec.due += trec.config['period'] }
    end

    def get_config(name)
      # Defaults
      #
      # A config has the following fields:
      # period - seconds
      # msg - msg for the queue
      #
      queue_config = {}

      if not File.exist? "timers/#{name}.json"
        return nil
      end

      json = File.read("timers/#{name}.json") rescue nil

      if json.nil?
        $log.warn("cannot read timers/#{name}.json")
        return nil
      end

      obj = JSON.parse(json) rescue nil

      if obj.nil?
        $log.warn("invalid json in timers/#{name}.json")
        return nil
      end

      config = obj

      if not config.has_key?('period')
        $log.warn("missing 'period' field in timers/#{name}.json")
        return nil
      end

      if config['period'].class != Fixnum
        $log.warn("invalid 'period' field in timers/#{name}.json")
        return nil
      end
      if config['period'] < 60
        $log.warn("'period' field too small in timers/#{name}.json")
        return nil
      end

      if not config.has_key?('msg')
        $log.warn("missing 'msg' field in timers/#{name}.json")
        return nil
      end

      if not config['msg'].has_key?('dest')
        $log.warn("missing 'msg.dest' field in timers/#{name}.json")
        return nil
      end

      config
    end

    def shutdown!
      exit!(0)
    end
  end
end

