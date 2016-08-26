
#
# Run
#
# Watch directory every so often
#
# Create and down queues as necessary
#


require 'fileutils'

require 'code/queuename'
require 'code/queuemgrclient'


module RQ

  class ScandirRecord < Struct.new(
    :config_stat,   # File.stat
    :config,        # JSON converted to Ruby objects
    :action         # :none, :new, :reload, :shutdown
  )
  end

  class Scandir

    def self.run!

      # Look at each symlink in scandir
      # If the directory has an executable 'run' file and an optional config.json...
      # use that for an rq 'que'. The queue always has a separate directory since it needs
      # directories for prep, que, run, etc. Otherwise, it would have to put them into the
      # possibly 'git' versioned directory with the source 'run' and 'config.json' and that
      # would be messy.
      #
      # In detail, if there is a config.json in there, use it as initial config
      # If not, still generate a config.json with default values
      #
      # If a new queue, create a que
      # If a config.json changed, notify a que
      # If a running que is no longer in scandir, shutdown a que
      #
      # In the future, we would only run this if the directory changed
      # or the symlinked 'config.json' (if present), changed

      # Wait for queue mgr to start things
      # TODO: why not ask queuemgr for state instead
      sleep 5
      $log.info("rq-scandir running")

      # Seed the system
      known_queues = Scandir.existing_queues()
      # que_name -> ScandirRecord

      while true
        # Get list of directories in 'scandir'
        names = Dir.entries('scandir').select do |x|
          RQ::QueueName::valid_queue_name(x) &&
            File.readable?(File.join('scandir', x)) &&
            File.symlink?(File.join('scandir', x)) &&
            File.directory?(File.join('scandir', x)) &&
            File.executable?(File.join('scandir', x, 'run'))
        end

        # TODO
        # See if existing queues match what is known
        # (maybe a user did something)
        # If a queue was deleted, we may not notice 
        # If a queue was added, we will attempt to create

        # Of those, validate and generate a config.json
        new_queues = {}

        names.each {
          |que_name|

          sdrec = known_queues[que_name]
          if sdrec
            stat = File.stat("scandir/#{que_name}/config.json") rescue nil
            if sdrec.config_stat == stat
              sdrec.action = :none
              new_queues[que_name] = sdrec
            else
              config = Scandir::get_config(que_name)
              if config
                sdrec.config = config
                sdrec.action = :reload
                sdrec.config_stat = stat
                new_queues[que_name] = sdrec
              else
                # We put an error in the log, we could just
                # put the existing back in, but that would violate
                # a principle. Fail fast and never hide errors
                # By not adding it, it is as if the symlink was removed
              end
            end
          else
            sdrec = ScandirRecord.new

            config = Scandir::get_config(que_name)
            if config
              sdrec.config = config
              sdrec.action = :new
              new_queues[que_name] = sdrec
            end
          end
        }

        qmgr = RQ::QueueMgrClient.new
        exit 1 unless qmgr.running?

        # Shutdown any queues no longer in scandir (or with bad json)
        delete_list = []
        known_queues.each {
          |k,sdrec|
          if not new_queues.has_key?(k)
            delete_list << k
            sdrec.action = :shutdown
            new_queues[k] = sdrec
          end
        }

        # Perform actions on new_queue
        #
        new_queues.each {
          |que_name, sdrec|

          next if sdrec.action == :none 

          config_path = File.join('queue', que_name, 'config.json')

          if sdrec.action == :reload 
            $log.info("reload que - #{que_name}")
            Scandir::safe_write(config_path, sdrec.config.to_json)
            qmgr.restart_queue(que_name)
          elsif sdrec.action == :shutdown 
            $log.info("shutdown que - #{que_name}")
            qmgr.delete_queue(que_name)
          elsif sdrec.action == :new 
            $log.info("create que - #{que_name}")
            res = qmgr.create_queue(sdrec.config)
            $log.info("create que - #{que_name} - result: #{res}")
            if res == ['fail', 'already created']
              res2 = qmgr.up_queue(que_name)
              $log.info("create que - #{que_name} -> UP result: #{res2}")
            end
            real_path = File.readlink("scandir/#{que_name}")
            FileUtils.symlink("../" + File.join(real_path, 'form.json'), "queue/#{que_name}/form.json", :force => true)
          end

          sdrec.action = :none
        }

        delete_list.each {|e| new_queues.delete(e) }

        known_queues = new_queues

        sleep 15
      end
    end

    def self.existing_queues
      qmgr = RQ::QueueMgrClient.new
      exit 1 unless qmgr.running?
      result = {}
      qmgr.queues.each {
        |que_name|
        sdrec = ScandirRecord.new
        sdrec.config = Scandir::get_config(que_name)
        sdrec.config_stat = File.stat("scandir/#{que_name}/config.json") rescue nil
        sdrec.action = :none
        result[que_name] = sdrec
      }
      result
    end

    def self.safe_write(path, data)
      File.write(path + ".tmp", data)
      File.rename(path + ".tmp", path)
    end

    def self.get_config(que_name)
      # Defaults
      queue_config = {}

      if not File.exist? "scandir/#{que_name}"
        return nil
      end

      real_path = File.readlink("scandir/#{que_name}")

      data = File.read("scandir/#{que_name}/config.json") rescue nil

      if data
        json = JSON.parse(data) rescue nil

        if json.nil?
          $log.warn("#{que_name} has invalid json in config.json - SKIPPING - !!")
          return nil
        end

        queue_config = json
      end

      queue_config['name'] = que_name
      queue_config['script'] = File.realpath("scandir/#{que_name}/run")
      queue_config['num_workers'] ||= '1'
      queue_config['exec_prefix'] ||= ''

      queue_config
    end

  end
end

