class Turboquery::TableMover

  attr_accessor :source, :destination, :from_table, :to_table

  def initialize(source:, destination:, from_table:, to_table:)
    self.source = source
    self.destination = destination
    self.from_table = from_table
    self.to_table = to_table
  end

  def move
    create_destination unless destination_exists?
    key = copy_source_to_s3
    copy_s3_to_destination(key)
  end

  private

  def source_exists?
    source.table_exists?(from_table)
  end

  def destination_exists?
    destination.table_exists?(to_table)
  end

  def create_destination
    structure = source.dump_table_ddl(from_table)
    structure.gsub!(/^CREATE TABLE [^\(]+\(/, "CREATE TABLE #{to_table} (")
    destination.execute(structure)
    fail 'Unable to create destination table' unless destination_exists?
  end

  def copy_source_to_s3
    source.copy_table_to_s3(from_table)
  end

  def copy_s3_to_destination(key)
    destination.copy_s3_to_table(key, to_table)
  end

end
