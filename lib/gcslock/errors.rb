module GCSLock
  class Error < StandardError; end
  class LockAlreadyOwnedError < Error; end
  class LockNotOwnedError < Error; end
  class LockNotFoundError < Error; end
  class LockTimeoutError < Error; end
end
