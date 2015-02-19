require 'tempfile'

class Turboquery::OLTP < Turboquery::Connection
  def copy_table_to_s3(table)
    set_env

    temp = Tempfile.new('turboquery', Turboquery.tmp_path)
    command = "echo 'COPY #{table} TO STDOUT' | psql #{Shellwords.escape(config[:database])} > #{temp.path}"
    Kernel.system(command)

    key = random_key
    copy_file_to_s3(temp.path, key)
    upload_manifest(key)
    key
  end

  def copy_s3_to_table(key, table)
    set_env
    path = copy_s3_to_tmp(key)
    sql = "COPY #{table} FROM STDIN"
    command = "cat #{Shellwords.escape(path)} | psql -c #{Shellwords.escape(sql)} #{Shellwords.escape(config[:database])}"
    Kernel.system(command)
  end

  def self.after_fork
    AROLTP.reconnect
  end

  protected

  def connection
    AROLTP.connection
  end

  class AROLTP < ActiveRecord::Base
    establish_connection DatabaseUrl.new(Turboquery.oltp_database_url).to_hash

    def self.reconnect
      establish_connection DatabaseUrl.new(Turboquery.oltp_database_url).to_hash
    end
  end
end
