require 'google/cloud/storage'
require 'securerandom'

require_relative 'errors'
require_relative 'mutex'
require_relative 'utils'

module GCSLock
  class Semaphore
    def initialize(bucket, object, count, client: nil, uuid: nil, min_backoff: nil, max_backoff: nil)
      @client = client || Google::Cloud::Storage.new
      @bucket = bucket
      @object = object
      @count = count

      @uuid = uuid || SecureRandom.uuid
      @min_backoff = min_backoff || 0.01
      @max_backoff = max_backoff || 5.0

      @permits = []
    end

    # Attempts to grab permits and waits if it isn't available.
    #
    # @param permits [Integer] the number of permits to acquire
    # @param timeout [Integer] the duration to wait before cancelling the operation
    #   if the lock was not obtained (unlimited if _nil_).
    # @param permits_to_check [Integer] the number of permits to check for acquisition
    #   until the required number of permits is secured for each iteration
    #   (defaults to _nil_, all permits if _nil_)
    #
    # @return [Boolean] `true` if the lock was obtained.
    #
    # @raise [LockAlreadyOwnedError] if the permit is already owned by the current instance.
    # @raise [LockTimeoutError] if the permits were not obtained before reaching the timeout.
    def acquire(permits: 1, timeout: nil, permits_to_check: nil)
      begin
        Utils.backoff(min_backoff: @min_backoff, max_backoff: @max_backoff, timeout: timeout) do
          try_acquire(permits: permits, permits_to_check: permits_to_check)
        end
      rescue LockTimeoutError
        raise LockTimeoutError, "Unable to get semaphore permit for #{@object} before timeout"
      end
    end

    # Attempts to obtain a permit and returns immediately.
    #
    # @param permits [Integer] the number of permits to acquire
    # @param permits_to_check [Integer] the number of permits to check for acquisition
    #   until the required number of permits is secured (defaults to _nil_, all permits if _nil_)
    #
    # @return [Boolean] `true` if the requested number of permits was granted.
    def try_acquire(permits: 1, permits_to_check: nil)
      acquired = []

      @count.times.to_a.sample(permits_to_check || @count).each do |index|
        mutex = mutex_object(index: index)
        if mutex.try_lock
          acquired.push(mutex)
          break if acquired.size == permits
        end
      end

      if acquired.size < permits
        acquired.each { |mutex| mutex.unlock }
        return false
      end

      @permits.push(*acquired)
      true
    end

    # Releases the given number of permits.
    #
    # @param permits [Integer] the number of permits to acquire
    #
    # @return _nil_
    #
    # @raise [LockNotOwnedError] if the permit is not owned by the current instance.
    def release(permits: 1)
      permits.times do
        raise LockNotOwnedError, "No semaphore for #{@object} is owned by this process" unless @permits&.any?

        @permits.pop.unlock
      end

      nil
    end

    # Releases all of the owned permits.
    #
    # @return _nil_
    #
    # @raise [LockNotOwnedError] if the permit is not owned by the current instance.
    def release_all
      while @permits&.any?
        @permits.pop.unlock
      end

      nil
    end

    # Force releases all of the permits in the semaphore, even if not owned.
    #
    # @return _nil_
    def release_all!
      mutexes = @count.times.map { |index| mutex_object(index: index) }
      mutexes.each do |mut|
        mut.unlock!
      rescue LockNotFoundError
        nil
      end

      @permits = []

      nil
    end

    # Acquires and returns all permits that are immediately available.
    #
    # @return [Integer] The number of permits acquired
    def drain_permits
      mutexes = @count.times.map { |index| mutex_object(index: index) }
      mutexes.select! { |mutex| mutex.try_lock }

      @permits.push(*mutexes)

      mutexes.size
    end

    # Returns the current number of permits available for this semaphore.
    #
    # @return [Integer] The number of permits available
    def available_permits
      mutexes = @count.times.map { |index| mutex_object(index: index) }
      mutexes.select! { |mutex| !mutex.locked? }

      mutexes.size
    end

    # Returns the current number of permits owned by this process for this semaphore.
    #
    # @return [Integer] The number of permits owned by this process
    def owned_permits
      @permits.select! { |mutex| mutex.owned? }
      @permits.size
    end

    private

    def mutex_object(index: nil)
      GCSLock::Mutex.new(
        @bucket, "#{@object}.#{index.nil? ? rand(@count) : index}",
        client: @client,
        uuid: @uuid,
        min_backoff: @min_backoff,
        max_backoff: @max_backoff,
      )
    end
  end
end
