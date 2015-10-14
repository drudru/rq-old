
module RQ

  class QueueName

    # Validate characters in name
    # No '.' or '/' since that could change path
    # Basically it should just be alphanum and '-' or '_'
    def self.valid_queue_name(name)
      return false unless name
      return false unless name.length > 0

      nil == name.tr('/. ,;:@"(){}\\+=\'^`#~?[]%|$&<>', '*').index('*')
    end

  end
end

