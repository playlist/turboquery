module Turboquery
  class << self
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
          region: 'us-east-1',
          access_key_id: s3_config['access_key_id'],
          secret_access_key: s3_config['secret_access_key']
        ).bucket(s3_config['bucket'])
      end
    end

    def s3_config
      Rails.application.secrets.turboquery
    end

    def tmp_path
      ENV['TURBOQUERY_TMP_PATH'] || Rails.root.join('tmp')
    end

    def after_fork
      olap.after_fork
      oltp.after_fork
    end
  end
end
