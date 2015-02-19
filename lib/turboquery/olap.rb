class Turboquery::OLAP < Turboquery::Connection
  def copy_table_to_s3(table)
    copy_result_to_s3("SELECT * FROM #{table}")
  end

  def copy_result_to_s3(query)
    key = random_key
    execute("UNLOAD ('#{query}') TO 's3://#{Turboquery.aws_bucket}/#{key}' #{copy_options};")
    key
  end

  def copy_s3_to_table(key, table)
    execute("COPY #{table} FROM 's3://#{Turboquery.aws_bucket}/#{key}manifest' #{copy_options}
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
    AROLAP.connect
  end

  protected

  def connection
    AROLAP.connection
  end

  def excape_single_quotes(str)
    str.gsub(/'/, %q(\\\'))
  end

  def copy_options
    "CREDENTIALS 'aws_access_key_id=#{Turboquery.aws_key};aws_secret_access_key=#{Turboquery.aws_secret}'
     MANIFEST DELIMITER '\\t' NULL AS '\\\\N'"
  end

  class AROLAP < ActiveRecord::Base
    def self.connect
      establish_connection Turboquery::DatabaseUrl.new(Turboquery.olap_database_url).to_hash
    end
  end
end
