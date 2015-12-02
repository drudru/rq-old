
module SimplePC

  # Simple Procedure Call
  class BasePacket
    attr_accessor :cmd
    attr_accessor :payload

    attr_accessor :sock
    attr_accessor :data
    attr_accessor :state
    attr_accessor :timeout
    attr_accessor :start_time

    @@states = [ :new, :recv_header, :recv_payload, :send, :done, :timeout, :err ]

    def initialize(sock)
      @sock = sock
      @data = ''
      @state = :new
      @timeout = 5
      @start_time = Time.now.to_i
    end

    def timed_out?

      return if [ :new, :done, :timeout, :err ].include? @state

      elapsed_time = Time.now.to_i - @start_time

      if elapsed_time > @timeout
        @state = :timeout
        return true
      end
      false
    end

  end

  class RecvPacket < BasePacket

    attr_accessor :num_to_read
    attr_accessor :err_reason

    def sockets
      [ @sock, nil ]
    end

    def process_io()
      if @state == :new
        @num_to_read = 256
        @state = :recv_header
      end

      return if timed_out?

      begin
        data = @sock.read_nonblock(@num_to_read)
      rescue Errno::EINTR
        return
      rescue IO::WaitReadable
        return
      rescue EOFError
        data = nil
      end

      if data.nil?
        @state = :err
        @err_reason = :eof
        return
      end
      @data += data

      if @state == :recv_header

        return if @data.bytesize < 13

        if (@data[0..3] != 'rq2 ')
          @state = :err
          @err_reason = :protocol
          return
        end

        if (@data[4..11] !~ /\d{8,8}/)
          @state = :err
          @err_reason = :protocol_size
          return
        end

        @payload_size = @data[4..11].to_i(10)
        @state = :recv_payload
      end

      if @state == :recv_payload
        diff = @data.bytesize - (@payload_size + 13)
        if diff == 0
          obj = JSON.parse(@data[13..-1])
          @cmd = obj[0]
          @payload = obj[1]
          @state = :done
          return
        elsif diff < 0
          @num_to_read = -diff
          return
        else
          # Ok - we got more than we should get
          # this must be an error
          @state = :err
          @err_reason = :protocol_too_much
          return
        end
      end
    end

  end

  class SendPacket < BasePacket

    attr_accessor :orig_data

    def sockets
      [ nil, @sock ]
    end

    def self.send(sock, cmd, payload)
      pkt = SendPacket.new(sock)
      json = [cmd, payload].to_json;
      pkt.data = sprintf("rq2 %08d %s", json.bytesize, json)
      pkt.orig_data = pkt.data
      pkt
    end

    def process_io()
      if @state == :new
        @state = :send
      end

      return if timed_out?

      begin
        len = @sock.write_nonblock(@data)
      rescue Errno::EINTR
        return
      rescue IO::WaitWritable
        return
      rescue EOFError
        len = nil
      end

      if len.nil?
        @state = :err
        return
      end

      if len == @data.bytesize
        @state = :done
        return
      end

      @data = @data.byteslice(len..-1)
    end

  end

end

