class Turboquery::RemoteQuery

  INSERT_INTO_REGEX = /^INSERT INTO (.+) SELECT/i

  attr_reader :query, :opts, :destination

  def initialize(query, opts = {})
    @query = query.dup
    @table_mapping = opts[:table_mapping] || {}
    @reverse_table_mapping = {}
    @copy_tables = opts[:copy_tables] || []
    @opts = opts
    format_query
  end

  def to_sql
    query
  end

  def execute
    # Copy any missing tables or requested tables
    (missing_tables + @copy_tables).uniq.each { |t| copy_local_table_to_remote(t) }

    # Run query and save to temporary table
    execute_query

    # Export results if no destination specified
    return Turboquery.olap.execute("SELECT * FROM #{temp_table_key}") unless @destination

    # Copy data from OLAP to OLTP
    copy_remote_table_to_local(temp_table_key, @destination)

    # Clean up temporary table
    Turboquery.olap.execute("DROP TABLE #{temp_table_key}")

    # Return success
    true
  end

  private

  def temp_table_key
    @temp_table_key ||= "turboquery_#{SecureRandom.hex(10)}"
  end

  def insert_into
    @insert_into ||= begin
      match = query.match(INSERT_INTO_REGEX)
      match.nil? ? nil : match[1]
    end
  end

  def insert_into?
    !insert_into.nil?
  end

  def format_query
    @destination = insert_into || opts[:destination]
    strip_insert_into if insert_into?
    build_table_tokens
    build_table_mapping
    build_reverse_table_mapping
    prefix_tables
  end

  def build_table_tokens
    @table_tokens = locate_tables(PgQuery.parse(@query).parsetree[0])
  end

  def locate_tables(parsetree)
    tables = []

    return [] unless parsetree.is_a? Hash

    parsetree.each do |key, val|
      tables << { table: val, location: parsetree['location'] } if key == 'relname'
      tables << locate_tables(val) if val.is_a? Hash
      tables << val.map { |v| locate_tables(v) } if val.is_a? Array
    end

    tables.flatten
  end

  def build_table_mapping
    @table_tokens.each do |table|
      @table_mapping[table[:table]] ||= "turboquery_#{table[:table]}"
    end
  end

  def build_reverse_table_mapping
    @table_mapping.each do |local, remote|
      @reverse_table_mapping[remote] = local
    end
  end

  def prefix_tables
    @table_tokens.reverse.each do |table|
      @query[table[:location], table[:table].length] = @table_mapping[table[:table]]
    end
  end

  def strip_insert_into
    @query.gsub! INSERT_INTO_REGEX, 'SELECT'
  end

  def local_tables
    @table_mapping.map { |k, _v| k }
  end

  def remote_tables
    @table_mapping.map { |_k, v| v }
  end

  def missing_tables
    remote_tables.select { |t| !Turboquery.olap.table_exists?(t) }
  end

  def copy_local_table_to_remote(table)
    Turboquery::TableMover.new(
      source: Turboquery.oltp,
      destination: Turboquery.olap,
      from_table: @reverse_table_mapping[table],
      to_table: table
    ).move
  end

  def copy_remote_table_to_local(remote, local)
    Turboquery::TableMover.new(
      source: Turboquery.olap,
      destination: Turboquery.oltp,
      from_table: remote,
      to_table: local
    ).move
  end

  def execute_query
    if @destination
      Turboquery.olap.execute("CREATE TABLE #{temp_table_key} AS #{@query}")
    else
      Turboquery.olap.execute("CREATE TEMPORARY TABLE #{temp_table_key} AS #{@query}")
    end
  end

end
