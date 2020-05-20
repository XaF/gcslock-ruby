require 'spec_helper'
require 'gcslock/mutex'

describe GCSLock::Mutex do
  before do
    @bucket_name = 'bucket'
    @object_name = 'object'

    @gcs = instance_double(Google::Cloud::Storage::Project)
    allow(Google::Cloud::Storage).to receive(:new).and_return(@gcs)

    @bucket = instance_double(Google::Cloud::Storage::Bucket)
    allow(@bucket).to receive(:name).and_return(@bucket_name)
    allow(@gcs).to receive(:bucket).and_return(@bucket)

    @object = instance_double(Google::Cloud::Storage::File)
    allow(@object).to receive(:name).and_return(@object_name)
    allow(@bucket).to receive(:file).and_return(@object)

    @uuid = 'some_uuid'
    allow(SecureRandom).to receive(:uuid).and_return(@uuid)
  end

  describe '.initialize' do
    before do
      @gcs = double(Google::Cloud::Storage)
      allow(Google::Cloud::Storage).to receive(:new).and_return(@gcs)

      @bucket = double(Google::Cloud::Storage::Bucket)
      allow(@gcs).to receive(:bucket).and_return(@bucket)

      @object = double(Google::Cloud::Storage::File)
      allow(@bucket).to receive(:file).and_return(@object)
    end

    it 'initializes in GCS client when none provided' do
      expect(Google::Cloud::Storage).to receive(:new).once

      GCSLock::Mutex.new(@bucket_name, @object_name)
    end

    it 'initializes in GCS client when none provided' do
      expect(Google::Cloud::Storage).not_to receive(:new)

      GCSLock::Mutex.new(@bucket_name, @object_name, client: @gcs)
    end

    it 'initializes a bucket with lazy loading' do
      expect(Google::Cloud::Storage).to receive(:new).once
      expect(@gcs).to receive(:bucket).with(@bucket_name, skip_lookup: true).once

      GCSLock::Mutex.new(@bucket_name, @object_name)
    end

    it 'initializes a file with lazy loading' do
      expect(Google::Cloud::Storage).to receive(:new).once
      expect(@bucket).to receive(:file).with(@object_name, skip_lookup: true).once

      GCSLock::Mutex.new(@bucket_name, @object_name)
    end

    it 'initializes a randomly generated unique ID' do
      expect(SecureRandom).to receive(:uuid).once

      GCSLock::Mutex.new(@bucket_name, @object_name)
    end
  end

  context 'initialized' do
    before do
      @mutex = GCSLock::Mutex.new(@bucket_name, @object_name)
    end

    describe '.lock' do
      it 'sleeps and retry when failing on the first try_lock' do
        expect(@mutex).to receive(:owned?).once.and_return(false)
        expect(@mutex).to receive(:try_lock).once.and_return(false)
        expect(@mutex).to receive(:sleep).once
        expect(@mutex).to receive(:try_lock).once.and_return(true)

        @mutex.lock(timeout: 2)
      end

      it 'sleeps just the time needed to retry once at the end' do
        expect(@mutex).to receive(:owned?).once.and_return(false)
        expect(@mutex).to receive(:sleep).exactly(2).times
        expect(@mutex).to receive(:try_lock).exactly(3).times.and_return(false)

        expect do
          @mutex.lock(timeout: 0.03)
        end.to raise_error(GCSLock::LockTimeoutError)
      end

      it 'raises an error if unable to get the lock when reaching the timeout' do
        expect(@mutex).to receive(:owned?).once.and_return(false)
        expect(@mutex).to receive(:try_lock).once.and_return(false)

        expect do
          @mutex.lock(timeout: 0)
        end.to raise_error(GCSLock::LockTimeoutError)
      end

      it 'raises an error if the lock is already owned' do
        expect(@mutex).to receive(:owned?).once.and_return(true)
        expect(@mutex).not_to receive(:try_lock)

        expect do
          @mutex.lock
        end.to raise_error(GCSLock::LockAlreadyOwnedError)
      end
    end

    describe '.locked?' do
      before do
        allow(@object).to receive(:reload!)
        allow(@object).to receive(:exists?)
      end

      it 'calls reload! on the lock file' do
        expect(@object).to receive(:reload!).once

        @mutex.locked?
      end

      it 'calls exists? on the lock file' do
        expect(@object).to receive(:exists?).once

        @mutex.locked?
      end

      it 'returns true if exists is true' do
        expect(@object).to receive(:exists?).once.and_return(true)

        expect(@mutex.locked?).to be(true)
      end

      it 'returns false if exists is false' do
        expect(@object).to receive(:exists?).once.and_return(false)

        expect(@mutex.locked?).to be(false)
      end
    end

    describe '.owned?' do
      it 'returns false if mutex is not locked' do
        expect(@mutex).to receive(:locked?).once.and_return(false)

        expect(@mutex.owned?).to be(false)
      end

      it 'returns false if mutex is locked but object size != uuid size' do
        expect(@mutex).to receive(:locked?).once.and_return(true)
        expect(@object).to receive(:size).once.and_return(@uuid.size + 10)

        expect(@mutex.owned?).to be(false)
      end

      it 'returns false if mutex is locked and object size == uuid size but object content != uuid' do
        expect(@mutex).to receive(:locked?).once.and_return(true)
        expect(@object).to receive(:size).once.and_return(@uuid.size)

        download = StringIO.new('blah')
        expect(@object).to receive(:download).once.and_return(download)

        expect(@mutex.owned?).to be(false)
      end

      it 'returns true if mutex is locked and object contains uuid' do
        expect(@mutex).to receive(:locked?).once.and_return(true)
        expect(@object).to receive(:size).once.and_return(@uuid.size)

        download = StringIO.new(@uuid)
        expect(@object).to receive(:download).once.and_return(download)

        expect(@mutex.owned?).to be(true)
      end
    end

    describe '.synchronize' do
      it 'locks, yields and unlock the mutex' do
        expect(@mutex).to receive(:owned?).once.and_return(false)
        expect(@mutex).to receive(:lock).once.and_return(true)
        expect(@mutex).to receive(:unlock).once

        has_yielded = false
        @mutex.synchronize do
          has_yielded = true
        end

        expect(has_yielded).to be(true)
      end

      it 'raises an error if the lock is already owned' do
        expect(@mutex).to receive(:owned?).once.and_return(true)
        expect(@mutex).not_to receive(:lock)
        expect(@mutex).not_to receive(:unlock)

        has_yielded = false

        expect do
          @mutex.synchronize do
            has_yielded = true
          end
        end.to raise_error(GCSLock::LockAlreadyOwnedError)

        expect(has_yielded).to be(false)
      end
    end

    describe '.try_lock' do
      before do
        @service = instance_double(Google::Cloud::Storage::Service)
        allow(@gcs).to receive(:service).and_return(@service)

        @servicev1 = instance_double(Google::Apis::StorageV1::StorageService)
        allow(@service).to receive(:service).and_return(@servicev1)
      end

      it 'returns true if lock obtained' do
        expect(@servicev1).to receive(:insert_object).with(
          @bucket_name,
          name: @object_name,
          if_generation_match: 0,
          upload_source: instance_of(StringIO),
        ).once

        expect(@mutex.try_lock).to be(true)
      end

      it 'returns false if lock already taken (precondition failed)' do
        client_error = Google::Apis::ClientError.new('conditionNotMet: Precondition failed', status_code: 412)

        expect(@servicev1).to receive(:insert_object).with(
          @bucket_name,
          name: @object_name,
          if_generation_match: 0,
          upload_source: instance_of(StringIO),
        ).once.and_raise(client_error)

        expect(@mutex.try_lock).to be(false)
      end

      it 'raises in case of precondition failed for other reason than conditionNotMet' do
        client_error = Google::Apis::ClientError.new('blah: Precondition failed', status_code: 412)

        expect(@servicev1).to receive(:insert_object).with(
          @bucket_name,
          name: @object_name,
          if_generation_match: 0,
          upload_source: instance_of(StringIO),
        ).once.and_raise(client_error)

        expect do
          @mutex.try_lock
        end.to raise_error(client_error)
      end

      it 'raises in case of other error than precondition failed' do
        client_error = Google::Apis::ClientError.new('blah', status_code: 400)

        expect(@servicev1).to receive(:insert_object).with(
          @bucket_name,
          name: @object_name,
          if_generation_match: 0,
          upload_source: instance_of(StringIO),
        ).once.and_raise(client_error)

        expect do
          @mutex.try_lock
        end.to raise_error(client_error)
      end
    end

    describe '.unlock' do
      it 'calls delete on the object if lock is owned' do
        expect(@mutex).to receive(:owned?).once.and_return(true)
        expect(@object).to receive(:delete).once

        @mutex.unlock
      end

      it 'raises an error if the lock is not owned' do
        expect(@mutex).to receive(:owned?).once.and_return(false)
        expect(@object).not_to receive(:delete)

        expect do
          @mutex.unlock
        end.to raise_error(GCSLock::LockNotOwnedError)
      end
    end

    describe '.unlock!' do
      it 'calls delete on the object' do
        expect(@object).to receive(:delete).once

        @mutex.unlock!
      end

      it 'raises an error if the object is not found' do
        expect(@object).to receive(:delete).once.and_raise(Google::Cloud::NotFoundError.new('blah'))

        expect do
          @mutex.unlock!
        end.to raise_error(GCSLock::LockNotFoundError)
      end
    end
  end
end
