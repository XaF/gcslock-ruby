# gcslock-ruby

This is inspired by [the Golang version](https://github.com/marcacohen/gcslock).

## Google Cloud Storage setup

1. Setup a new project at the [Google APIs Console](https://console.developers.google.com) and enable the Cloud Storage API.
1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/downloads) tool and configure your project and your OAuth credentials.
1. Create a bucket in which to store your lock file using the command `gsutil mb gs://your-bucket-name`.
1. Enable object versioning in your bucket using the command `gsutil versioning set on gs://your-bucket-name`.
1. In your Ruby code, require `gcslock/mutex` and use it as follows:

```ruby
require 'gcslock/mutex'

m = GCSLock::Mutex.new('your-bucket-name', 'my-file.lock')
m.synchronize do
  # Protected and globally serialized computation happens here.
end
```

