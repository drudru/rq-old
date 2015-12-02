
module RQ
  class RQUNIXSocket

    attr_accessor :path
    attr_accessor :sock

    def self.start_server(path)
      File.unlink(path) rescue nil
      sock = UNIXServer.open(path)

      sock
    end

    def self.do_accept(sock)
      begin
        client_socket, client_sockaddr = sock.accept_nonblock
        return client_socket, client_sockaddr
      rescue Errno::EINTR
        $log.warn('error EINTR on accept')
      rescue Errno::EWOULDBLOCK, Errno::ECONNABORTED, Errno::EPROTO
        $log.warn("#{$!} error on accept")
      end
      return nil,nil
    end

  end

end

