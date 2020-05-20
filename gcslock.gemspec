lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gcslock/version'

Gem::Specification.new do |spec|
  spec.name = 'gcslock'
  spec.version = GCSLock::VERSION
  spec.authors = ['Raphaël Beamonte']
  spec.email = ['raphael.beamonte@gmail.com']

  spec.summary = 'Google Cloud Storage distributed locking'
  spec.description = "Allows to use a Google Cloud Storage bucket as a distributed locking system"
  spec.homepage = 'https://github.com/XaF/gcslock-ruby'
  spec.license = 'MIT'

  raise "RubyGems 2.0 or newer is required to protect against public gem pushes." unless spec.respond_to?(:metadata)
  spec.metadata['allowed_push_host'] = "https://rubygems.org"

  spec.files = %x(git ls-files -z).split("\x0").reject { |f| f.match(/^(test|DESIGN)/) }
  spec.require_paths = ['lib']

  spec.required_ruby_version = '>= 2.0'

  spec.add_runtime_dependency 'google-api-client'
  spec.add_runtime_dependency 'google-cloud-storage', '>= 1.24.0'

  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'codecov'
end
