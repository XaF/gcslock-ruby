require 'spec_helper'
require 'gcslock/semaphore'

describe GCSLock::Semaphore do
  before do
    @bucket_name = 'bucket'
    @object_name = 'object'
    @count = 5

    @gcs = instance_double(Google::Cloud::Storage::Project)
    allow(Google::Cloud::Storage).to receive(:new).and_return(@gcs)

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

      GCSLock::Semaphore.new(@bucket_name, @object_name, @count)
    end

    it 'does not initialize a GCS client when none provided' do
      expect(Google::Cloud::Storage).not_to receive(:new)

      GCSLock::Semaphore.new(@bucket_name, @object_name, @count, client: @gcs)
    end

    it 'initializes a randomly generated unique ID' do
      expect(SecureRandom).to receive(:uuid).once

      GCSLock::Semaphore.new(@bucket_name, @object_name, @count)
    end
  end

  context 'initialized' do
    before do
      @sem = GCSLock::Semaphore.new(@bucket_name, @object_name, @count)
    end

    describe '.acquire' do
      it 'sleeps and retry when failing on the first try_acquire' do
        expect(@sem).to receive(:try_acquire).once.and_return(false)
        expect(GCSLock::Utils).to receive(:sleep).once
        expect(@sem).to receive(:try_acquire).once.and_return(true)

        @sem.acquire(timeout: 2)
      end

      it 'sleeps just the time needed to retry once at the end' do
        expect(GCSLock::Utils).to receive(:sleep).at_least(2).times
        expect(@sem).to receive(:try_acquire).at_least(3).times.and_return(false)

        expect do
          @sem.acquire(timeout: 0.03)
        end.to raise_error(GCSLock::LockTimeoutError)
      end

      it 'raises an error if unable to get a permit when reaching the timeout' do
        expect(@sem).to receive(:try_acquire).once.and_return(false)

        expect do
          @sem.acquire(timeout: 0)
        end.to raise_error(GCSLock::LockTimeoutError)
      end

      it 'calls try_acquire with the right value for permits_to_check if passed as argument' do
        permits_to_check = 2

        expect(@sem).to receive(:try_acquire).with(permits: 1, permits_to_check: permits_to_check).once.and_return(true)

        @sem.acquire(permits_to_check: permits_to_check)
      end
    end

    describe '.try_acquire' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          allow(@sem).to receive(:mutex_object).with(index: index).and_return(mutex)

          mutex
        end
      end

      it 'returns true if permit (1) obtained, while all mutex available' do
        @mutexes.each do |mutex|
          allow(mutex).to receive(:try_lock).and_return(true)
        end

        expect(@sem.try_acquire).to be(true)
      end

      it 'returns true if permit (1) obtained, while only a single mutex available' do
        @mutexes.each_with_index do |mutex, index|
          allow(mutex).to receive(:try_lock).and_return(index == 2)
        end

        expect(@sem.try_acquire).to be(true)
      end

      it 'returns true if permit (3) obtained, while all mutex available' do
        @mutexes.each do |mutex|
          allow(mutex).to receive(:try_lock).and_return(true)
        end

        expect(@sem.try_acquire(permits: 3)).to be(true)
      end

      it 'returns true if permit (3) obtained, while only a single mutex available' do
        available_mutexes = @count.times.to_a.sample(3)

        @mutexes.each_with_index do |mutex, index|
          allow(mutex).to receive(:try_lock).and_return(available_mutexes.include?(index))
        end

        expect(@sem.try_acquire(permits: 3)).to be(true)
      end

      it 'returns false if no permit available' do
        @mutexes.each_with_index do |mutex, index|
          allow(mutex).to receive(:try_lock).and_return(false)
        end

        expect(@sem.try_acquire).to be(false)
      end

      it 'returns false if not enough permits available' do
        available_mutexes = @count.times.to_a.sample(2)

        @mutexes.each_with_index do |mutex, index|
          allow(mutex).to receive(:try_lock).and_return(available_mutexes.include?(index))
          expect(mutex).to receive(:unlock).once if available_mutexes.include?(index)
        end

        expect(@sem.try_acquire(permits: 3)).to be(false)
      end
    end

    describe '.release' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          mutex
        end

        @permits = @count.times.to_a.sample(3).map { |index| @mutexes[index] }

        @sem.instance_variable_set(:@permits, @permits)
      end

      it 'releases the permits (1) when called' do
        expect(@permits.last).to receive(:unlock).once

        @sem.release

        expect(@sem.instance_variable_get(:@permits).size).to eq(2)
      end

      it 'releases the permits (3) when called' do
        @permits.each do |mutex|
          expect(mutex).to receive(:unlock).once
        end

        @sem.release(permits: 3)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end

      it 'raises an error when releasing more permits (4) than owned (3), but still released the owned locks' do
        @permits.each do |mutex|
          expect(mutex).to receive(:unlock).once
        end

        expect do
          @sem.release(permits: 4)
        end.to raise_error(GCSLock::LockNotOwnedError)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end
    end

    describe '.release_all' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          mutex
        end

        @permits = @count.times.to_a.sample(3).map { |index| @mutexes[index] }

        @sem.instance_variable_set(:@permits, @permits)
      end

      it 'releases all owned permits when called' do
        @permits.each do |mutex|
          expect(mutex).to receive(:unlock).once
        end

        @sem.release_all

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end
    end

    describe '.release_all!' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          allow(@sem).to receive(:mutex_object).with(index: index).and_return(mutex)

          mutex
        end

        @permits = @count.times.to_a.sample(3).map { |index| @mutexes[index] }

        @sem.instance_variable_set(:@permits, @permits)
      end

      it 'releases all permits when called' do
        @mutexes.each do |mutex|
          expect(mutex).to receive(:unlock!).once
        end

        @sem.release_all!

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end
    end

    describe '.drain_permits' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          allow(@sem).to receive(:mutex_object).with(index: index).and_return(mutex)

          mutex
        end
      end

      it 'gets all the available permits (all)' do
        @mutexes.each do |mutex|
          expect(mutex).to receive(:try_lock).and_return(true)
        end

        expect(@sem.drain_permits).to eq(@count)

        expect(@sem.instance_variable_get(:@permits).size).to eq(@count)
      end

      it 'gets all the available permits (2) when already owning some' do
        owned_permits = @count.times.to_a.sample(3)
        permits = owned_permits.map { |index| @mutexes[index] }
        @sem.instance_variable_set(:@permits, permits)

        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:try_lock).and_return(!owned_permits.include?(index))
        end

        expect(@sem.drain_permits).to eq(@count - owned_permits.size)

        expect(@sem.instance_variable_get(:@permits).size).to eq(@count)
      end

      it 'gets all available permits (2) when somebody else is already owning some' do
        owned_permits = @count.times.to_a.sample(3)

        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:try_lock).and_return(!owned_permits.include?(index))
        end

        expect(@sem.drain_permits).to eq(@count - owned_permits.size)

        expect(@sem.instance_variable_get(:@permits).size).to eq(@count - owned_permits.size)
      end

      it 'gets nothing when all the permits are owned' do
        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:try_lock).and_return(false)
        end

        expect(@sem.drain_permits).to eq(0)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end
    end

    describe '.available_permits' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          allow(@sem).to receive(:mutex_object).with(index: index).and_return(mutex)

          mutex
        end
      end

      it 'returns the number of available permits (all)' do
        @mutexes.each do |mutex|
          expect(mutex).to receive(:locked?).and_return(false)
        end

        expect(@sem.available_permits).to eq(@count)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end

      it 'returns the number of available permits (2) when already owning some' do
        owned_permits = @count.times.to_a.sample(3)
        permits = owned_permits.map { |index| @mutexes[index] }
        @sem.instance_variable_set(:@permits, permits.dup)

        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:locked?).and_return(owned_permits.include?(index))
        end

        expect(@sem.available_permits).to eq(@count - owned_permits.size)

        expect(@sem.instance_variable_get(:@permits)).to eq(permits)
      end

      it 'returns the number of available permits (2) when somebody else is already owning some' do
        owned_permits = @count.times.to_a.sample(3)

        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:locked?).and_return(owned_permits.include?(index))
        end

        expect(@sem.available_permits).to eq(@count - owned_permits.size)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end

      it 'returns 0 when all the permits are owned' do
        @mutexes.each_with_index do |mutex, index|
          expect(mutex).to receive(:locked?).and_return(true)
        end

        expect(@sem.available_permits).to eq(0)

        expect(@sem.instance_variable_get(:@permits).size).to eq(0)
      end
    end

    describe '.owned_permits' do
      before do
        @mutexes = @count.times.map do |index|
          mutex = instance_double(GCSLock::Mutex)

          mutex
        end
      end

      it 'returns the owned permits after checking they are properly owned (all owned)' do
        owned_permits = @count.times.to_a.sample(3)
        permits = owned_permits.map { |index| @mutexes[index] }
        @sem.instance_variable_set(:@permits, permits.dup)

        permits.each do |mutex|
          expect(mutex).to receive(:owned?).and_return(true)
        end

        expect(@sem.owned_permits).to eq(owned_permits.size)
        expect(@sem.instance_variable_get(:@permits)).to eq(permits)
      end

      it 'returns the owned permits after checking they are properly owned (one not owned)' do
        owned_permits = @count.times.to_a.sample(3)
        permits = owned_permits.map { |index| @mutexes[index] }
        not_really_owned = permits.sample
        @sem.instance_variable_set(:@permits, permits.dup)

        permits.each do |mutex|
          expect(mutex).to receive(:owned?).and_return(mutex != not_really_owned)
        end

        expect(@sem.owned_permits).to eq(owned_permits.size - 1)
        expect(@sem.instance_variable_get(:@permits)).to eq(permits.select { |mutex| mutex != not_really_owned })
      end
    end

  end
end
