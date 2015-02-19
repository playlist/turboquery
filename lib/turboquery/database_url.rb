require 'uri'
require 'cgi'

class Turboquery::DatabaseUrl
  def initialize(url = nil)
    @uri = URI.parse(url || ENV['DATABASE_URL'])
    @query = CGI.parse @uri.query.to_s
  end

  def to_hash
    build_hash
    @hash
  end

  private

  def build_hash
    @hash = {
      adapter: @uri.scheme,
      host: @uri.host,
      database: File.basename(@uri.path)
    }

    set_optional :port, @uri.port
    set_optional :username, @uri.user
    set_optional :password, @uri.password
    set_optional :encoding, encoding
    set_optional :pool, pool
  end

  def set_optional(key, val)
    @hash[key] = val if val
  end

  def encoding
    @query.key?('encoding') ? @query['encoding'][0] : nil
  end

  def pool
    pool = @query['pool'] || @query['max_connections']
    pool.length > 0 ? pool[0].to_i : nil
  end
end
