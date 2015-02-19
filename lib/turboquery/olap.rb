class Turboquery::OLAP < Turboquery::Connection
  def copy_table_to_s3(table)
    copy_result_to_s3("SELECT * FROM #{table}")
  end

  def copy_result_to_s3(query)
    key = random_key
    execute("UNLOAD ('#{query}') TO 's3://#{Turboquery.s3_config['bucket']}/#{key}' #{copy_options};")
    key
  end

  def copy_s3_to_table(key, table)
    execute("COPY #{table} FROM 's3://#{Turboquery.s3_config['bucket']}/#{key}manifest' #{copy_options}
    DATEFORMAT 'auto' TIMEFORMAT 'auto';")
  end

  def execute_to_temporary_table(query)
    key = random_key
    sql = "CREATE TEMPORARY TABLE #{key} AS #{query}"
    execute(sql)
    key
  end

  def drop_table(key)
    sql = "DROP TABLE IF EXISTS #{key}"
    execute(sql)
  end

  def self.after_fork
    ARRedshift.reconnect
  end

  protected

  def connection
    ARRedshift.connection
  end

  def excape_single_quotes(str)
    str.gsub(/'/, %q(\\\'))
  end

  def copy_options
    "CREDENTIALS 'aws_access_key_id=#{Turboquery.s3_config['access_key_id']};aws_secret_access_key=#{Turboquery.s3_config['secret_access_key']}'
     MANIFEST DELIMITER '\\t' NULL AS '\\\\N'"
  end

  class ARRedshift < ActiveRecord::Base
    establish_connection DatabaseUrl.new(ENV['REDSHIFT_DATABASE_URL']).to_hash

    def self.reconnect
      establish_connection DatabaseUrl.new(ENV['REDSHIFT_DATABASE_URL']).to_hash
    end
  end
end
