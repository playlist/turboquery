require File.expand_path('../lib/turboquery/version', __FILE__)

Gem::Specification.new do |spec|
  spec.name          = 'turboquery'
  spec.version       = Turboquery::VERSION
  spec.authors       = ['Jacob Gillespie']
  spec.email         = ['jacobwgillespie@gmail.com']
  spec.description   = 'Execute with the power of Redshift'
  spec.summary       = 'Turboquery allows you to execute your Postgres queries on your Postgres data with Redshift.'
  spec.homepage      = 'https://github.com/jacobwgillespie/turboquery'
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split("\n")
  spec.executables   = spec.files.grep(/^bin\//) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(/^(test|spec|features)\//)
  spec.require_paths = ['lib']

  spec.add_dependency 'activerecord', '~> 4.0'
  spec.add_dependency 'pg_query'

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'minitest'
end
