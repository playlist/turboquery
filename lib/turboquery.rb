require 'active_record'
require 'pg_query'

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
  end

  self.oltp_database_url = ENV['TURBOQUERY_OLTP_DATABASE_URL']
  self.olap_database_url = ENV['TURBOQUERY_OLAP_DATABASE_URL']
  self.aws_key = ENV['TURBOQUERY_AWS_KEY']
  self.aws_secret = ENV['TURBOQUERY_AWS_SECRET']
  self.aws_bucket = ENV['TURBOQUERY_AWS_BUCKET']
  self.aws_region = ENV['TURBOQUERY_AWS_REGION'] || 'us-east-1'
  self.tmp_path = ENV['TURBOQUERY_TMP_PATH']
end
