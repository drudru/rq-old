
module RQ
  class Proc

    attr_accessor :pid
    attr_accessor :name
    attr_accessor :child_write_pipe

    def self.start_process(name)
      proc_obj = Proc.new

      # nice pipes writeup
      # http://www.cim.mcgill.ca/~franco/OpSys-304-427/lecture-notes/node28.html
      child_rd, parent_wr = IO.pipe

      child_pid = fork do
        # Restore default signal handlers from those inherited
        Signal.trap('TERM', 'DEFAULT')
        Signal.trap('CHLD', 'DEFAULT')
        Signal.trap('HUP', 'DEFAULT')

        $0 = $log.progname = "[rq-#{name}]"
        begin
          parent_wr.close
          #child only code block
          $log.debug('post fork')

          yield(child_rd)
          # This should never return, it should Kernel.exit!
          raise

        rescue Exception
          $log.error("Exception!")
          $log.error($!)
          $log.error($!.backtrace)
          raise
        end
      end

      #parent only code block
      child_rd.close

      if child_pid == nil
        parent_wr.close
        return nil
      end

      proc_obj.pid = child_pid
      proc_obj.name = name
      proc_obj.child_write_pipe = parent_wr

      proc_obj
      # If anything went wrong at all log it and return nil.
    rescue Exception
      $log.error("Failed to start process #{name}: #{$!}")
      nil
    end
  end

end

