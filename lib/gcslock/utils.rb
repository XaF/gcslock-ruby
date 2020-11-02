module GCSLock
  class Utils
    class << self
      def backoff(min_backoff:, max_backoff:, timeout: nil)
        backoff = min_backoff

        now = Time.now
        end_time = now + timeout unless timeout.nil?

        loop do
          return true if yield
          break if !timeout.nil? && now + backoff >= end_time
          sleep(backoff)

          backoff_opts = [max_backoff, backoff * 2]

          unless timeout.nil?
            now = Time.now
            diff = end_time - now
            backoff_opts.push(diff) if diff > 0
          end

          backoff = backoff_opts.min
        end

        raise LockTimeoutError, "Backoff timed out"
      end
    end
  end
end

