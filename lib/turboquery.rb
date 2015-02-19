require 'active_record'
require 'pg_query'
require 'aws-sdk'
require 'active_record/connection_adapters/redshift_adapter'
require 'pg'

module Turboquery
end

require 'turboquery/database_url'
require 'turboquery/connection'
require 'turboquery/oltp'
require 'turboquery/olap'
require 'turboquery/remote_query'
require 'turboquery/table_mover'
require 'turboquery/version'

module Turboquery
  class << self
    attr_accessor :oltp_database_url
    attr_accessor :olap_database_url
    attr_accessor :aws_key
    attr_accessor :aws_secret
    attr_accessor :aws_bucket
    attr_accessor :aws_region
    attr_accessor :tmp_path

    def query(sql, opts = {})
      Turboquery::RemoteQuery.new(sql, opts).execute
    end

    def update_table(table)
      Turboquery::TableMover.new(source: oltp, destination: olap, from_table: table, to_table: "turboquery_#{table}")
    end

    def oltp
      @oltp ||= Turboquery::OLTP.new
    end

    def olap
      @olap ||= Turboquery::OLAP.new
    end

    def s3_bucket
      @s3_bucket ||= begin
        Aws::S3::Resource.new(
          region: aws_region,
          access_key_id: aws_key,
          secret_access_key: aws_secret
        ).bucket(aws_bucket)
      end
    end

    def after_fork
      olap.after_fork
      oltp.after_fork
    end

    def oltp_database_url
      @oltp_database_url || ENV['TURBOQUERY_OLTP_DATABASE_URL']
    end

    def olap_database_url
      @olap_database_url || ENV['TURBOQUERY_OLAP_DATABASE_URL']
    end

    def aws_key
      @aws_key || ENV['TURBOQUERY_AWS_KEY']
    end

    def aws_secret
      @aws_secret || ENV['TURBOQUERY_AWS_SECRET']
    end

    def aws_bucket
      @aws_bucket || ENV['TURBOQUERY_AWS_BUCKET']
    end

    def aws_region
      @aws_region || ENV['TURBOQUERY_AWS_REGION'] || 'us-east-1'
    end

    def tmp_path
      @tmp_path || ENV['TURBOQUERY_TMP_PATH']
    end
  end

  oltp.after_fork if oltp_database_url
  olap.after_fork if olap_database_url

  def self.config
    yield self
    after_fork
  end
end
