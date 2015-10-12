
#
# Run
#
# Watch directory every so often
#
# Create and down queues as necessary
#


require 'socket'
require 'fcntl'
require 'fileutils'

module RQ

  class Scandir

    def self.run!
      while true
        $log.warn("Running!")
        sleep 10
      end
    end

  end
end

