require 'google/cloud/storage'
require 'securerandom'

require_relative 'errors'

module GCSLock
  class Mutex
    def initialize(bucket, object, client: nil, uuid: nil, min_backoff: nil, max_backoff: nil)
      @client = client || Google::Cloud::Storage.new
      @bucket = @client.bucket(bucket, skip_lookup: true)
      @object = @bucket.file(object, skip_lookup: true)

      @uuid = uuid || SecureRandom.uuid
      @min_backoff = min_backoff || 0.01
      @max_backoff = max_backoff || 5.0
    end

    # Attempts to grab the lock and waits if it isn't available.
    # Raises `ThreadError` if `mutex` was locked by the current thread.
    def lock(timeout: nil)
      raise LockAlreadyOwnedError, "Mutex for #{@object.name} is already owned by this process" if owned?

      backoff = @min_backoff

      now = Time.now
      end_time = now + timeout unless timeout.nil?

      loop do
        return true if try_lock
        break if !timeout.nil? && now + backoff >= end_time
        sleep(backoff)

        backoff_opts = [@max_backoff, backoff * 2]

        unless timeout.nil?
          now = Time.now
          diff = end_time - now
          backoff_opts.push(diff) if diff > 0
        end

        backoff = backoff_opts.min
      end

      raise LockTimeoutError, "Unable to get mutex for #{@object.name} before timeout"
    end

    # Returns `true` if this lock is currently held by some thread.
    def locked?
      @object.reload!
      @object.exists?
    rescue Google::Cloud::NotFoundError
      false
    end

    # Returns `true` if this lock is currently held by current thread.
    def owned?
      locked? && @object.size == @uuid.size && @object.download.read == @uuid
    end

    # Obtains a lock, runs the block, and releases the lock when the block completes.
    # Raises `LockAlreadyOwnedError` if the lock is already owned by the current instance.
    def synchronize(timeout: nil)
      raise LockAlreadyOwnedError, "Mutex for #{@object.name} is already owned by this process" if owned?

      lock(timeout: timeout)
      begin
        yield
      ensure
        unlock
      end
    end

    # Attempts to obtain the lock and returns immediately. Returns `true` if the lock was granted.
    def try_lock
      @client.service.service.insert_object(
        @bucket.name,
        name: @object.name,
        if_generation_match: 0,
        upload_source: StringIO.new(@uuid),
      )

      true
    rescue Google::Apis::ClientError => e
      raise unless e.status_code == 412 && e.message.start_with?('conditionNotMet:')

      false
    end

    # Releases the lock. Raises `LockNotOwnedError` if the lock is not owned by the current instance.
    def unlock
      raise LockNotOwnedError, "Mutex for #{@object.name} is not owned by this process" unless owned?
      @object.delete
    end

    # Releases the lock even if not owned by this instance. Raises `LockNotFoundError` if the lock cannot be found.
    def unlock!
      @object.delete
    rescue Google::Cloud::NotFoundError => e
      raise LockNotFoundError, "Mutex for #{@object.name} not found"
    end
  end
end
