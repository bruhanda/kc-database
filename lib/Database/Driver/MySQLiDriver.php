<?php 

namespace KC\Database\Driver;

use KC\Database\Database;
use KC\Database\DatabaseException;
use mysqli;

/**
 * MySQLi driver
 */
class MySQLiDriver extends Database
{
  // Identifier for this connection within the PHP driver
  protected $connectionId;
  
  // MySQL uses a backtick for identifiers
  protected $identifier = '`';
  
  public function connect()
  {
    if ($this->connection) {
      return;
    }
  
    // Add required variables
    $vars = $this->config['connection'] + [
      'database' => '',
      'hostname' => '',
      'username' => '',
      'password' => '',
      'socket'   => '',
      'port'     => 3306,
      'ssl'      => NULL,
    ];
    // Extract the connection parameters
    extract($vars);
    // Prevent this information from showing up in traces
    unset($this->config['connection']['username'], $this->config['connection']['password']);
  
    try {
      if (is_array($ssl)) {
        $this->connection = mysqli_init();
        $this->connection->ssl_set(
          Arr::get($ssl, 'client_key_path'),
          Arr::get($ssl, 'client_cert_path'),
          Arr::get($ssl, 'ca_cert_path'),
          Arr::get($ssl, 'ca_dir_path'),
          Arr::get($ssl, 'cipher')
        );
        $this->connection->real_connect($hostname, $username, $password, $database, $port, $socket, MYSQLI_CLIENT_SSL);
      } else {
        $this->connection = new mysqli($hostname, $username, $password, $database, $port, $socket);
      }
    } catch (Exception $e) {
      // No connection exists
      $this->connection = null;
      throw new DatabaseException($e->getMessage(), $e->getCode(), $e);
    }
  
    // \xFF is a better delimiter, but the PHP driver uses underscore
    $this->connectionId = sha1($hostname.'_'.$username.'_'.$password);
  
    if (!empty($this->config['charset'])) {
      // Set the character set
      $this->setCharset($this->config['charset']);
    }
    if (!empty($this->config['connection']['variables'])) {
      // Set session variables
      $variables = [];
      foreach ($this->config['connection']['variables'] as $var => $val) {
        $variables[] = 'SESSION '.$var.' = '.$this->quote($val);
      }
  
      $this->connection->query('SET '.implode(', ', $variables));
    }
  }
  
  public function disconnect()
  {
    try {
      // Database is assumed disconnected
      $status = true;
      if (is_resource($this->connection)) {
        if ($status = $this->connection->close()) {
          // Clear the connection
          $this->connection = null;
        }
      }
    } catch (Exception $e) {
      // Database is probably not disconnected
      $status = (!is_resource($this->connection));
    }
  
    return $status;
  }
  
  public function setCharset($charset)
  {
    // Make sure the database is connected
    $this->connect();
    // PHP is compiled against MySQL 5.x
    $status = $this->connection->set_charset($charset);
  
    if ($status === false) {
      throw new DatabaseException("{$this->connection->errno}:{$this->connection->error}");
    }
  }
  
  public function query($type, $sql, $asObject = false, array $params = null)
  {
    // Make sure the database is connected
    $this->connect();
    // Execute the query
    if (($result = $this->connection->query($sql)) === false) {
      throw new DatabaseException("{$this->connection->errno}:{$this->connection->error} [ {$sql} ]");
    }
    // Set the last query
    $this->lastQuery = $sql;
  
    if ($type === Database::SELECT) {
      // Return an iterator of results
      return new MySQLiResult($result, $sql, $asObject, $params);
    } elseif ($type === Database::INSERT) {
      // Return a list of insert id and rows created
      return [$this->connection->insert_id, $this->connection->affected_rows];
    } else {
      // Return the number of rows affected
      return $this->connection->affected_rows;
    }
  }
  
  public function datatype($type)
  {
    static $types = [
      'blob'                      => ['type' => 'string', 'binary' => true, 'character_maximum_length' => '65535'],
      'bool'                      => ['type' => 'bool'],
      'bigint unsigned'           => ['type' => 'int', 'min' => '0', 'max' => '18446744073709551615'],
      'datetime'                  => ['type' => 'string'],
      'decimal unsigned'          => ['type' => 'float', 'exact' => true, 'min' => '0'],
      'double'                    => ['type' => 'float'],
      'double precision unsigned' => ['type' => 'float', 'min' => '0'],
      'double unsigned'           => ['type' => 'float', 'min' => '0'],
      'enum'                      => ['type' => 'string'],
      'fixed'                     => ['type' => 'float', 'exact' => true],
      'fixed unsigned'            => ['type' => 'float', 'exact' => true, 'min' => '0'],
      'float unsigned'            => ['type' => 'float', 'min' => '0'],
      'geometry'                  => ['type' => 'string', 'binary' => true],
      'int unsigned'              => ['type' => 'int', 'min' => '0', 'max' => '4294967295'],
      'integer unsigned'          => ['type' => 'int', 'min' => '0', 'max' => '4294967295'],
      'longblob'                  => ['type' => 'string', 'binary' => true, 'character_maximum_length' => '4294967295'],
      'longtext'                  => ['type' => 'string', 'character_maximum_length' => '4294967295'],
      'mediumblob'                => ['type' => 'string', 'binary' => true, 'character_maximum_length' => '16777215'],
      'mediumint'                 => ['type' => 'int', 'min' => '-8388608', 'max' => '8388607'],
      'mediumint unsigned'        => ['type' => 'int', 'min' => '0', 'max' => '16777215'],
      'mediumtext'                => ['type' => 'string', 'character_maximum_length' => '16777215'],
      'national varchar'          => ['type' => 'string'],
      'numeric unsigned'          => ['type' => 'float', 'exact' => true, 'min' => '0'],
      'nvarchar'                  => ['type' => 'string'],
      'point'                     => ['type' => 'string', 'binary' => true],
      'real unsigned'             => ['type' => 'float', 'min' => '0'],
      'set'                       => ['type' => 'string'],
      'smallint unsigned'         => ['type' => 'int', 'min' => '0', 'max' => '65535'],
      'text'                      => ['type' => 'string', 'character_maximum_length' => '65535'],
      'tinyblob'                  => ['type' => 'string', 'binary' => true, 'character_maximum_length' => '255'],
      'tinyint'                   => ['type' => 'int', 'min' => '-128', 'max' => '127'],
      'tinyint unsigned'          => ['type' => 'int', 'min' => '0', 'max' => '255'],
      'tinytext'                  => ['type' => 'string', 'character_maximum_length' => '255'],
      'year'                      => ['type' => 'string']
    ];
  
    $type = str_replace(' zerofill', '', $type);
  
    if (isset($types[$type])) {
      return $types[$type];
    }
  
    return parent::datatype($type);
  }
  
  /**
   * Start a SQL transaction
   *
   * @link http://dev.mysql.com/doc/refman/5.0/en/set-transaction.html
   *
   * @param string $mode  Isolation level
   * @return boolean
   */
  public function begin($mode = null)
  {
    // Make sure the database is connected
    $this->connect();
  
    if ($mode && !$this->connection->query($sql = "SET TRANSACTION ISOLATION LEVEL {$mode}")) {
      throw new DatabaseException("{$this->connection->errno}:{$this->connection->error} [ {$sql} ]");
    }
  
    return (bool)$this->connection->query('START TRANSACTION');
  }
  
  /**
   * Commit a SQL transaction
   *
   * @return boolean
   */
  public function commit()
  {
    // Make sure the database is connected
    $this->connect();
  
    return (bool)$this->connection->query('COMMIT');
  }
  
  /**
   * Rollback a SQL transaction
   *
   * @return boolean
   */
  public function rollback()
  {
    // Make sure the database is connected
    $this->connect();
  
    return (bool)$this->connection->query('ROLLBACK');
  }
  
  public function listTables($like = null)
  {
    if (is_string($like)) {
      // Search for table names
      $result = $this->query(Database::SELECT, 'SHOW TABLES LIKE '.$this->quote($like), false);
    } else {
      // Find all table names
      $result = $this->query(Database::SELECT, 'SHOW TABLES', false);
    }
  
    $tables = [];
    foreach ($result as $row) {
      $tables[] = reset($row);
    }
  
    return $tables;
  }
  
  public function listColumns($table, $like = null, $addPrefix = true)
  {
    // Quote the table name
    $table = ($addPrefix === true) ? $this->quoteTable($table) : $table;
  
    if (is_string($like)) {
      // Search for column names
      $result = $this->query(Database::SELECT, 'SHOW FULL COLUMNS FROM '.$table.' LIKE '.$this->quote($like), false);
    } else {
      // Find all column names
      $result = $this->query(Database::SELECT, 'SHOW FULL COLUMNS FROM '.$table, false);
    }
  
    $count = 0;
    $columns = [];
    foreach ($result as $row) {
      list($type, $length) = $this->parseType($row['Type']);
      $column = $this->datatype($type);
  
      $column['column_name']      = $row['Field'];
      $column['column_default']   = $row['Default'];
      $column['data_type']        = $type;
      $column['is_nullable']      = ($row['Null'] == 'YES');
      $column['ordinal_position'] = ++$count;
  
      switch ($column['type']) {
        case 'float':
          if (isset($length)) {
            list($column['numeric_precision'], $column['numeric_scale']) = explode(',', $length);
          }
        break;
        case 'int':
          if (isset($length)) {
            // MySQL attribute
            $column['display'] = $length;
          }
        break;
        case 'string':
          switch ($column['data_type']) {
            case 'binary':
            case 'varbinary':
              $column['character_maximum_length'] = $length;
            break;
            case 'char':
            case 'varchar':
              $column['character_maximum_length'] = $length;
            case 'text':
            case 'tinytext':
            case 'mediumtext':
            case 'longtext':
              $column['collation_name'] = $row['Collation'];
            break;
            case 'enum':
            case 'set':
              $column['collation_name'] = $row['Collation'];
              $column['options'] = explode('\',\'', substr($length, 1, -1));
            break;
          }
        break;
      }
  
      // MySQL attributes
      $column['comment']      = $row['Comment'];
      $column['extra']        = $row['Extra'];
      $column['key']          = $row['Key'];
      $column['privileges']   = $row['Privileges'];
  
      $columns[$row['Field']] = $column;
    }
  
    return $columns;
  }
  
  public function escape($value)
  {
    // Make sure the database is connected
    $this->connect();
  
    if (($value = $this->connection->real_escape_string((string)$value)) === false) {
      throw new DatabaseException("{$this->connection->errno}:{$this->connection->error}");
    }
  
    // SQL standard is to use single-quotes for all values
    return "'$value'";
  }
}
