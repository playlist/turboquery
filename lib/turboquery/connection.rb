require 'shellwords'

class Turboquery::Connection

  def table_exists?(table = nil)
    return false if table.nil?
    connection.execute("
       SELECT 1 AS exists
       FROM   information_schema.tables
       WHERE  table_schema = 'public'
       AND    table_name = '#{table}';
    ").count == 1
  end

  def dump_table_ddl(table)
    set_env
    schema = `pg_dump -i -s -x -o -O --no-tablespaces --no-security-labels -t #{Shellwords.escape(table)} #{Shellwords.escape(config[:database])}`
    schema.empty? ? '' : schema.match(/CREATE TABLE[^;]+;/)[0]
  end

  def copy_table_to_s3(_table)
    fail 'Not implemented'
  end

  def copy_s3_to_table(_key, _table)
    fail 'Not implemented'
  end

  def execute(*args, &block)
    connection.execute(*args, &block)
  end

  def config
    connection.instance_variable_get(:@config)
  end

  def set_env
    PsqlEnv.set(config)
  end

  def after_fork
    self.class.after_fork
  end

  def self.after_fork
    AR.establish_connection
  end

  protected

  def connection
    AR.connection
  end

  def random_key
    SecureRandom.hex(10)
  end

  def valid_objects(key)
    Turboquery.s3_bucket.objects.to_a.select do |obj|
      obj.key =~ /^#{key}/ && !(obj.key =~ /manifest$/)
    end
  end

  def build_manifest(objects)
    {
      entries: objects.map do |obj|
        {
          url: "s3://#{obj.bucket_name}/#{obj.key}",
          mandatory: true
        }
      end
    }.to_json
  end

  def upload_manifest(key)
    objects = valid_objects(key)
    file = Tempfile.open('turboquery', Turboquery.tmp_path) do |f|
      f.write(build_manifest(objects))
      f
    end
    copy_file_to_s3(file.path, "#{key}manifest")
  end

  def copy_s3_to_tmp(key)
    objects = valid_objects(key)
    file = Tempfile.open('turboquery', Turboquery.tmp_path) do |f|
      objects.each do |object|
        f.write object.get.body.read
      end
      f
    end
    file.path
  end

  def copy_file_to_s3(filename, key)
    object = Turboquery.s3_bucket.object(key)
    object.upload_file(filename)
  end

  module PsqlEnv
    def self.set(config)
      host(config)     if config[:host]
      port(config)     if config[:port]
      password(config) if config[:password]
      username(config) if config[:username]
    end

    def self.host(config)
      ENV['PGHOST']     = config[:host]
    end

    def self.port(config)
      ENV['PGPORT']     = config[:port].to_s
    end

    def self.password(config)
      ENV['PGPASSWORD'] = config[:password].to_s
    end

    def self.username(config)
      ENV['PGUSER']     = config[:username].to_s
    end
  end

end
