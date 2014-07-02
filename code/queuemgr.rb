require 'socket'
require 'json'
require 'fcntl'

require 'code/queue'
require 'code/scheduler'
require 'code/web_server'
require 'code/protocol'
require 'version'

def log(mesg)
  File.open('log/queuemgr.log', "a") do |f|
    f.write("#{Process.pid} - #{Time.now} - #{mesg}\n")
  end
end

module RQ
  class QueueMgr
    include Protocol

    attr_accessor :queues
    attr_accessor :scheduler
    attr_accessor :web_server
    attr_accessor :status
    attr_accessor :environment

    def initialize
      @queues = { } # Hash of queue name => RQ::Queue object
      @queue_errs = Hash.new(0) # Hash of queue name => count of restarts, default 0
      @scheduler = nil
      @web_server = nil
      @start_time = Time.now
      @status = "RUNNING"
    end

    def load_config
      begin
        data = File.read('config/config.json')
        @config = JSON.parse(data)
        ENV["RQ_ENV"] = @config['env']
      rescue
        log("Bad config file. Exiting")
        exit! 1
      end

      if @config['tmpdir']
        dir = File.expand_path(@config['tmpdir'])
        if File.directory?(dir) and File.writable?(dir)
          # This will affect the class Tempfile, which is used by Rack
          ENV['TMPDIR'] = dir
        else
          log("Bad 'tmpdir' in config json [#{dir}]. Exiting")
          exit! 1
        end
      end

      @config
    end

    def init
      # Show pid
      File.unlink('config/queuemgr.pid') rescue nil
      File.open('config/queuemgr.pid', "w") do |f|
        f.write("#{Process.pid}\n")
      end

      # Setup IPC
      File.unlink('config/queuemgr.sock') rescue nil
      $sock = UNIXServer.open('config/queuemgr.sock')
    end

    # Validate characters in name
    # No '.' or '/' since that could change path
    # Basically it should just be alphanum and '-' or '_'
    def valid_queue_name(name)
      nil == name.tr('/. ,;:@"(){}\\+=\'^`#~?[]%|$&<>', '*').index('*')
    end

    def queue_dirs
      Dir.entries('queue').select do |x|
        valid_queue_name x and
        File.readable? File.join('queue', x, 'config.json')
      end
    end

    def handle_request(sock)
      packet = read_packet(sock)
      return unless packet

      cmd, arg = packet.split(' ', 2)
      log("REQ [ #{cmd} #{arg} ]")

      case cmd
      when 'ping'
        resp = [ 'pong' ].to_json
        send_packet(sock, resp)

      when 'environment'
        resp = [ ENV['RQ_ENV'] ].to_json
        send_packet(sock, resp)

      when 'version'
        resp = [ RQ_VER ].to_json
        send_packet(sock, resp)

      when 'queues'
        resp = @queues.keys.to_json
        send_packet(sock, resp)

      when 'uptime'
        resp = [(Time.now - @start_time).to_i, ].to_json
        send_packet(sock, resp)

      when 'restart_queue'
        stop_queue(arg)
        # Reset the error count because the queue was manually restarted
        @queue_errs.delete(arg)
        sleep(0.001)
        start_queue(arg)

        resp = ['ok', arg].to_json
        send_packet(sock, resp)

      when 'create_queue'
        options = JSON.parse(arg)
        # "queue"=>{"name"=>"local", "script"=>"local.rb", "num_workers"=>"1", ...}

        if @queues.has_key?(options['name'])
          resp = ['fail', 'already created'].to_json
        else
          if not valid_queue_name(options['name'])
            resp = ['fail', 'queue name has invalid characters'].to_json
          else
            resp = ['fail', 'queue not created'].to_json
            worker = RQ::Queue.create(options)
            if worker
              log("create_queue STARTED [ #{worker.name} - #{worker.pid} ]")
              @queues[worker.name] = worker
              resp = ['success', 'queue created - awesome'].to_json
            end
          end
        end
        send_packet(sock, resp)

      when 'create_queue_link'
        err = false

        begin
          options = JSON.parse(File.read(arg))
        rescue
          reason = "could not read queue config [ #{arg} ]: #{$!}"
          err = true
        end

        if not err
          err, reason = RQ::Queue.validate_options(options)
        end

        if not err
          if @queues.has_key?(options['name'])
            reason = 'queue is already running'
            err = true
          end
        end

        if not err
          if not valid_queue_name(options['name'])
            reason = 'queue name has invalid characters'
            err = true
          end
        end

        if not err
          worker = RQ::Queue.create(options, arg)
          log("create_queue STARTED [ #{worker.name} - #{worker.pid} ]")
          if worker
            @queues[worker.name] = worker
            reason = 'queue created - awesome'
            err = false
          else
            reason = 'queue not created'
            err = true
          end
        end

        resp = [ (err ? 'fail' : 'success'), reason ].to_json
        send_packet(sock, resp)

      when 'delete_queue'
        worker = @queues[arg]
        if worker
          worker.status = "DELETE"
          Process.kill("TERM", worker.pid) rescue nil
          status = 'ok'
          msg = 'started deleting queue'
        else
          status = 'fail'
          msg = 'no such queue'
        end
        resp = [ status, msg ].to_json
        send_packet(sock, resp)

      else
        resp = [ 'error' ].to_json
        send_packet(sock, resp)
      end
    end

    def reload
      # Stop queues whose configs have gone away
      dirs = Hash[queue_dirs.zip]

      # Notify running queues to reload configs
      @queues.values.each do |worker|
        if dirs.has_key? worker.name
          log("RELOAD [ #{worker.name} - #{worker.pid} ] - SENDING HUP")
          Process.kill("HUP", worker.pid) if worker.pid rescue nil
        else
          log("RELOAD [ #{worker.name} - #{worker.pid} ] - SENDING TERM")
          worker.status = "SHUTDOWN"
          Process.kill("TERM", worker.pid) if worker.pid rescue nil
        end
      end

      # Start new queues if new configs were added
      load_queues
    end

    def shutdown
      final_shutdown! if @queues.empty?

      # Remove non-running entries
      @queues.delete_if { |n, q| !q.pid }

      @queues.each do |n, q|
        q.status = "SHUTDOWN"

        begin
          Process.kill("TERM", q.pid) if q.pid
        rescue StandardError => e
          puts "#{q.pid} #{e.inspect}"
        end
      end
    end

    def final_shutdown!
      # Once all the queues are down, take the scheduler down
      # Process.kill("TERM", @scheduler.pid) if @scheduler.pid

      # Once all the queues are down, take the web server down
      Process.kill("TERM", @web_server) if @web_server

      # The actual shutdown happens when all procs are reaped
      File.unlink('config/queuemgr.pid') rescue nil
      $sock.close
      File.unlink('config/queuemgr.sock') rescue nil
      log("FINAL SHUTDOWN - EXITING")
      Process.exit! 0
    end

    def stop_queue(name)
      worker = @queues[name]
      worker.status = "SHUTDOWN"
      Process.kill("TERM", worker.pid) rescue nil
    end

    def start_queue(name)
      worker = RQ::Queue.start_process({'name' => name})
      if worker
        @queues[worker.name] = worker
        log("STARTED [ #{worker.name} - #{worker.pid} ]")
      end
    end

    def start_scheduler
      worker = RQ::Scheduler.start_process
      if worker
        @scheduler = worker
        log("STARTED [ #{worker.name} - #{worker.pid} ]")
      end
    end

    def start_webserver
      @web_server = fork do
        # Restore default signal handlers from those inherited from queuemgr
        Signal.trap('TERM', 'DEFAULT')
        Signal.trap('CHLD', 'DEFAULT')

        $0 = '[rq-web]'
        RQ::WebServer.new(@config).run!
      end
    end

    def load_queues
      # Skip dot dirs and queues already running
      queue_dirs.each do |name|
        next if @queues.has_key?(name)
        start_queue name
      end
    end

    def run!
      $0 = '[rq-mgr]'

      init
      load_config

      Signal.trap("TERM") do
        log("received TERM signal")
        shutdown
      end

      Signal.trap("CHLD") do
        log("received CHLD signal")
      end

      Signal.trap("HUP") do
        log("received HUP signal")
        reload
      end

      load_queues

      # TODO implement cron-like scheduler and start it up
      # start_scheduler

      start_webserver

      flag = File::NONBLOCK
      if defined?(Fcntl::F_GETFL)
        flag |= $sock.fcntl(Fcntl::F_GETFL)
      end
      $sock.fcntl(Fcntl::F_SETFL, flag)

      # Ye old event loop
      while true
        #log(queues.select { |i| i.status != "ERROR" }.map { |i| [i.name, i.child_write_pipe] }.inspect)
        io_list = @queues.values.select { |i| i.status != "ERROR" }.map { |i| i.child_write_pipe }
        io_list << $sock
        #log(io_list.inspect)
        log('sleeping')
        begin
          ready, _, _ = IO.select(io_list, nil, nil, 60)
        rescue SystemCallError, StandardError # SystemCallError is the parent for all Errno::EFOO exceptions
          sleep 0.001 # A tiny pause to prevent consuming all CPU
          log("error on SELECT #{$!}")
          closed_sockets = io_list.delete_if { |i| i.closed? }
          log("removing closed sockets #{closed_sockets.inspect} from io_list")
          retry
        end

        next unless ready

        ready.each do |io|
          if io.fileno == $sock.fileno
            begin
              client_socket, client_sockaddr = $sock.accept
            rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::ECONNABORTED, Errno::EPROTO, Errno::EINTR
              log('error acception on main sock, supposed to be readysleeping')
            end
            # Linux Doesn't inherit and BSD does... recomended behavior is to set again
            flag = 0xffffffff ^ File::NONBLOCK
            if defined?(Fcntl::F_GETFL)
              flag &= client_socket.fcntl(Fcntl::F_GETFL)
            end
            #log("Non Block Flag -> #{flag} == #{File::NONBLOCK}")
            client_socket.fcntl(Fcntl::F_SETFL, flag)
            handle_request(client_socket)
          else
            # probably a child pipe that closed
            worker = @queues.values.find do |i|
              if i.child_write_pipe
                i.child_write_pipe.fileno == io.fileno
              end
            end
            if worker
              res = Process.wait2(worker.pid, Process::WNOHANG)
              if res
                log("QUEUE PROC #{worker.name} of PID #{worker.pid} exited with status #{res[1]} - #{worker.status}")
                worker.child_write_pipe.close

                case worker.status
                when 'RUNNING'
                  if (@queue_errs[worker.name] += 1) > 10
                    log("FAILED [ #{worker.name} - too many restarts. Not restarting ]")
                    new_worker = RQ::Worker.new
                    new_worker.status = 'ERROR'
                    new_worker.name = worker.name
                    @queues[worker.name] = new_worker
                  else
                    worker = RQ::Queue.start_process(worker.options)
                    log("RESTARTED [ #{worker.name} - #{worker.pid} ]")
                    @queues[worker.name] = worker
                  end

                when 'DELETE'
                  RQ::Queue.delete(worker.name)
                  @queues.delete(worker.name)
                  log("DELETED [ #{worker.name} ]")

                when 'SHUTDOWN'
                  @queues.delete(worker.name)
                  if @queues.empty?
                    final_shutdown!
                  end

                else
                  log("STRANGE: queue #{worker.pid } status = #{worker.status}")
                end

              else
                log("EXITING: queue #{worker.pid} was not ready to be reaped #{res}")
              end

            else
              log("VERY STRANGE: got a read ready on an io that we don't track!")
            end

          end
        end

      end
    end

  end
end
