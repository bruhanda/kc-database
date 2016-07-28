<?php 

namespace KC\Database;

/**
 * Database connection wrapper/helper.
 *
 * You may get a database instance using the static factory `KC\Database::instance($config)`
 *
 * This class provides connection instance management via Database Drivers, as
 * well as quoting, escaping and other related functions. Querys are done using
 * [DatabaseQuery] and [QueryBuilder] objects, which can be easily
 * created using the [DB] helper class.
 */
abstract class Database implements QueryTypeAwareInterface
{
  /**
   * @var  string  the last query executed
   */
  public $lastQuery;

  // Character that is used to quote identifiers
  protected $identifier = '"';

  // Instance name
  protected $instance;

  // Raw server connection
  protected $connection;

  // Configuration array
  protected $config;
  
  // Default filesystem query cache setting
  protected $queryCachePath;
  
  // Default filesystem query cache setting
  protected $queryCacheLifetime;
  
  // Array-like object or dictionary from external caching system
  protected $cache;
  
  /**
   * Stores the database configuration locally and name the instance.
   *
   * [!!] This method cannot be accessed directly, you must use [Database::instance].
   *
   * @return  void
   */
  public function __construct($name = "default", array $config)
  {
    // Set the instance name
    $this->instance = $name;

    // Store the config locally
    $this->config = $config;

    if (empty($this->config['table_prefix'])) {
      $this->config['table_prefix'] = '';
    }
    
    $caching = array_merge(
      ['path' => $this->queryCacheTempPath($name), 'lifetime' => 0],
      Arr::get($this->config, 'query_cache', [])
    );
    
    $this->queryCachePath = $caching['path'];
    $this->queryCacheLifetime = $caching['lifetime'];
    
    if (Arr::like($cache = Arr::get($caching, 'cache'))) {
      $this->cache = $cache;
    }
  }
  
  /**
   * Returns the database instance name.
   *
   *     echo (string) $db;
   *
   * @return  string
   */
  public function __toString()
  {
    return $this->name();
  }
  
  /**
   * Returns the database instance name.
   *
   *     echo $db->name();
   *
   * @return  string
   */
  public function name()
  {
    return $this->instance;
  }

  /**
   * Connect to the database. This is called automatically when the first
   * query is executed.
   *
   *     $db->connect();
   *
   * @throws  DatabaseException
   * @return  void
   */
  abstract public function connect();

  /**
   * Set the connection character set. This is called automatically by [Database::connect].
   *
   *     $db->setCharset('utf8');
   *
   * @throws  DatabaseException
   * @param   string   $charset  character set name
   * @return  void
   */
  abstract public function setCharset($charset);

  /**
   * Perform an SQL query of the given type.
   *
   *     // Make a SELECT query and use objects for results
   *     $db->query(Database::SELECT, 'SELECT * FROM groups', true);
   *
   *     // Make a SELECT query and use "Model_User" for the results
   *     $db->query(Database::SELECT, 'SELECT * FROM users LIMIT 1', 'Model_User');
   *
   * @param   integer  $type       Database::SELECT, Database::INSERT, etc
   * @param   string   $sql        SQL query
   * @param   mixed    $as_object  result object class string, true for stdClass, false for assoc array
   * @param   array    $params     object construct parameters for result class
   * @return  object   DatabaseResult for SELECT queries
   * @return  array    list (insert id, row count) for INSERT queries
   * @return  integer  number of affected rows for all other queries
   */
  abstract public function query($type, $sql, $asObject = false, array $params = null);

  /**
   * Start a SQL transaction
   *
   *     // Start the transactions
   *     $db->begin();
   *
   *     try {
   *          DB::insert('users')->values($user1)...
   *          DB::insert('users')->values($user2)...
   *          // Insert successful commit the changes
   *          $db->commit();
   *     }
   *     catch (DatabaseException $e) {
   *          // Insert failed. Rolling back changes...
   *          $db->rollback();
   *     }
   *
   * @param string $mode  transaction mode
   * @return  boolean
   */
  abstract public function begin($mode = null);

  /**
   * Commit the current transaction
   *
   *     // Commit the database changes
   *     $db->commit();
   *
   * @return  boolean
   */
  abstract public function commit();

  /**
   * Abort the current transaction
   *
   *     // Undo the changes
   *     $db->rollback();
   *
   * @return  boolean
   */
  abstract public function rollback();

  /**
   * Count the number of records in a table.
   *
   *     // Get the total number of records in the "users" table
   *     $count = $db->countRecords('users');
   *
   * @param   mixed    $table  table name string or array(query, alias)
   * @return  integer
   */
  public function countRecords($table)
  {
    // Quote the table name
    $table = $this->quoteTable($table);
    $query = 'SELECT COUNT(*) AS total_row_count FROM '.$table;
    
    return $this->query(Database::SELECT, $query, false)->get('total_row_count');
  }

  /**
   * Returns a normalized array describing the SQL data type
   *
   *     $db->datatype('char');
   *
   * @param   string  $type  SQL data type
   * @return  array
   */
  public function datatype($type)
  {
    static $types = [
      // SQL-92
      'bit'                           => ['type' => 'string', 'exact' => true],
      'bit varying'                   => ['type' => 'string'],
      'char'                          => ['type' => 'string', 'exact' => true],
      'char varying'                  => ['type' => 'string'],
      'character'                     => ['type' => 'string', 'exact' => true],
      'character varying'             => ['type' => 'string'],
      'date'                          => ['type' => 'string'],
      'dec'                           => ['type' => 'float', 'exact' => true],
      'decimal'                       => ['type' => 'float', 'exact' => true],
      'double precision'              => ['type' => 'float'],
      'float'                         => ['type' => 'float'],
      'int'                           => ['type' => 'int', 'min' => '-2147483648', 'max' => '2147483647'],
      'integer'                       => ['type' => 'int', 'min' => '-2147483648', 'max' => '2147483647'],
      'interval'                      => ['type' => 'string'],
      'national char'                 => ['type' => 'string', 'exact' => true],
      'national char varying'         => ['type' => 'string'],
      'national character'            => ['type' => 'string', 'exact' => true],
      'national character varying'    => ['type' => 'string'],
      'nchar'                         => ['type' => 'string', 'exact' => true],
      'nchar varying'                 => ['type' => 'string'],
      'numeric'                       => ['type' => 'float', 'exact' => true],
      'real'                          => ['type' => 'float'],
      'smallint'                      => ['type' => 'int', 'min' => '-32768', 'max' => '32767'],
      'time'                          => ['type' => 'string'],
      'time with time zone'           => ['type' => 'string'],
      'timestamp'                     => ['type' => 'string'],
      'timestamp with time zone'      => ['type' => 'string'],
      'varchar'                       => ['type' => 'string'],

      // SQL:1999
      'binary large object'               => ['type' => 'string', 'binary' => true],
      'blob'                              => ['type' => 'string', 'binary' => true],
      'boolean'                           => ['type' => 'bool'],
      'char large object'                 => ['type' => 'string'],
      'character large object'            => ['type' => 'string'],
      'clob'                              => ['type' => 'string'],
      'national character large object'   => ['type' => 'string'],
      'nchar large object'                => ['type' => 'string'],
      'nclob'                             => ['type' => 'string'],
      'time without time zone'            => ['type' => 'string'],
      'timestamp without time zone'       => ['type' => 'string'],

      // SQL:2003
      'bigint'    => ['type' => 'int', 'min' => '-9223372036854775808', 'max' => '9223372036854775807'],

      // SQL:2008
      'binary'            => ['type' => 'string', 'binary' => true, 'exact' => true],
      'binary varying'    => ['type' => 'string', 'binary' => true],
      'varbinary'         => ['type' => 'string', 'binary' => true]
    ];

    if (isset($types[$type])) {
      return $types[$type];
    }

    return array();
  }

  /**
   * List all of the tables in the database. Optionally, a LIKE string can
   * be used to search for specific tables.
   *
   *     // Get all tables in the current database
   *     $tables = $db->listTables();
   *
   *     // Get all user-related tables
   *     $tables = $db->listTables('user%');
   *
   * @param   string   $like  table to search for
   * @return  array
   */
  abstract public function listTables($like = null);

  /**
   * Lists all of the columns in a table. Optionally, a LIKE string can be
   * used to search for specific fields.
   *
   *     // Get all columns from the "users" table
   *     $columns = $db->listColumns('users');
   *
   *     // Get all name-related columns
   *     $columns = $db->listColumns('users', '%name%');
   *
   *     // Get the columns from a table that doesn't use the table prefix
   *     $columns = $db->listColumns('users', null, false);
   *
   * @param   string  $table       table to get columns from
   * @param   string  $like        column to search for
   * @param   boolean $addPrefix   whether to add the table prefix automatically or not
   * @return  array
   */
  abstract public function listColumns($table, $like = null, $addPrefix = true);

  /**
   * Extracts the text between parentheses, if any.
   *
   *     // Returns: array('CHAR', '6')
   *     list($type, $length) = $db->parseType('CHAR(6)');
   *
   * @param   string  $type
   * @return  array   list containing the type and length, if any
   */
  protected function parseType($type)
  {
    if (($open = strpos($type, '(')) === false) {
      // No length specified
      return [$type, null];
    }
    // Closing parenthesis
    $close = strrpos($type, ')', $open);
    // Length without parentheses
    $length = substr($type, $open + 1, $close - 1 - $open);
    // Type without the length
    $type = substr($type, 0, $open).substr($type, $close + 1);

    return [$type, $length];
  }

  /**
   * Return the table prefix defined in the current configuration.
   *
   *     $prefix = $db->tablePrefix();
   *
   * @return  string
   */
  public function tablePrefix()
  {
    return $this->config['table_prefix'];
  }

  /**
   * Quote a value for an SQL query.
   *
   *     $db->quote(null);   // 'null'
   *     $db->quote(10);     // 10
   *     $db->quote('fred'); // 'fred'
   *
   * Objects passed to this function will be converted to strings.
   * [DatabaseExpression] objects will be compiled.
   * [DatabaseQuery] objects will be compiled and converted to a sub-query.
   * All other objects will be converted using the `__toString` method.
   *
   * @param   mixed   $value  any value to quote
   * @return  string
   * @uses    Database::escape
   */
  public function quote($value)
  {
    if ($value === null) {
      return 'NULL';
    } elseif ($value === true) {
      return "'1'";
    } elseif ($value === false) {
      return "'0'";
    } elseif (is_object($value)) {
      if ($value instanceof DatabaseQuery) {
        // Create a sub-query
        return '('.$value->compile($this).')';
      } elseif ($value instanceof DatabaseExpression) {
        // Compile the expression
        return $value->compile($this);
      } else {
        // Convert the object to a string
        return $this->quote((string)$value);
      }
    } elseif (is_array($value)) {
      return '('.implode(', ', array_map(array($this, __FUNCTION__), $value)).')';
    } elseif (is_int($value)) {
      return (int)$value;
    }
    elseif (is_float($value)) {
      // Convert to non-locale aware float to prevent possible commas
      return sprintf('%F', $value);
    }

    return $this->escape($value);
  }

  /**
   * Quote a database column name and add the table prefix if needed.
   *
   *     $column = $db->quoteColumn($column);
   *
   * You can also use SQL methods within identifiers.
   *
   *     $column = $db->quoteColumn(DB::expr('COUNT(`column`)'));
   *
   * Objects passed to this function will be converted to strings.
   * [DatabaseExpression] objects will be compiled.
   * [DatabaseQuery] objects will be compiled and converted to a sub-query.
   * All other objects will be converted using the `__toString` method.
   *
   * @param   mixed   $column  column name or array(column, alias)
   * @return  string
   * @uses    Database::quoteIdentifier
   * @uses    Database::tablePrefix
   */
  public function quoteColumn($column)
  {
    // Identifiers are escaped by repeating them
    $escapedIdentifier = $this->identifier.$this->identifier;

    if (is_array($column)) {
      list($column, $alias) = $column;
      $alias = str_replace($this->identifier, $escapedIdentifier, $alias);
    }

    if ($column instanceof DatabaseQuery) {
      // Create a sub-query
      $column = '('.$column->compile($this).')';
    } elseif ($column instanceof DatabaseExpression) {
      // Compile the expression
      $column = $column->compile($this);
    } else {
      // Convert to a string
      $column = (string) $column;
      $column = str_replace($this->identifier, $escapedIdentifier, $column);

      if ($column === '*') {
        return $column;
      } elseif (strpos($column, '.') !== false) {
        $parts = explode('.', $column);

        if ($prefix = $this->tablePrefix()) {
          // Get the offset of the table name, 2nd-to-last part
          $offset = count($parts) - 2;
          // Add the table prefix to the table name
          $parts[$offset] = $prefix.$parts[$offset];
        }

        foreach ($parts as & $part) {
          if ($part !== '*') {
            // Quote each of the parts
            $part = $this->identifier.$part.$this->identifier;
          }
        }

        $column = implode('.', $parts);
      } else {
        $column = $this->identifier.$column.$this->identifier;
      }
    }

    if (isset($alias)) {
      $column .= ' AS '.$this->identifier.$alias.$this->identifier;
    }

    return $column;
  }

  /**
   * Quote a database table name and adds the table prefix if needed.
   *
   *     $table = $db->quoteTable($table);
   *
   * Objects passed to this function will be converted to strings.
   * [Database_Expression] objects will be compiled.
   * [Database_Query] objects will be compiled and converted to a sub-query.
   * All other objects will be converted using the `__toString` method.
   *
   * @param   mixed   $table  table name or array(table, alias)
   * @return  string
   * @uses    Database::quoteIdentifier
   * @uses    Database::tablePrefix
   */
  public function quoteTable($table)
  {
    // Identifiers are escaped by repeating them
    $escapedIdentifier = $this->identifier.$this->identifier;

    if (is_array($table)) {
      list($table, $alias) = $table;
      $alias = str_replace($this->identifier, $escapedIdentifier, $alias);
    }

    if ($table instanceof DatabaseQuery) {
      // Create a sub-query
      $table = '('.$table->compile($this).')';
    } elseif ($table instanceof DatabaseExpression) {
      // Compile the expression
      $table = $table->compile($this);
    } else {
      // Convert to a string
      $table = (string) $table;
      $table = str_replace($this->identifier, $escapedIdentifier, $table);

      if (strpos($table, '.') !== false) {
        $parts = explode('.', $table);

        if ($prefix = $this->tablePrefix()) {
          // Get the offset of the table name, last part
          $offset = count($parts) - 1;
          // Add the table prefix to the table name
          $parts[$offset] = $prefix.$parts[$offset];
        }

        foreach ($parts as & $part) {
          // Quote each of the parts
          $part = $this->identifier.$part.$this->identifier;
        }

        $table = implode('.', $parts);
      } else {
        // Add the table prefix
        $table = $this->identifier.$this->tablePrefix().$table.$this->identifier;
      }
    }

    if (isset($alias)) {
      // Attach table prefix to alias
      $table .= ' AS '.$this->identifier.$this->tablePrefix().$alias.$this->identifier;
    }

    return $table;
  }

  /**
   * Quote a database identifier
   *
   * Objects passed to this function will be converted to strings.
   * [DatabaseExpression] objects will be compiled.
   * [DatabaseQuery] objects will be compiled and converted to a sub-query.
   * All other objects will be converted using the `__toString` method.
   *
   * @param   mixed   $value  any identifier
   * @return  string
   */
  public function quoteIdentifier($value)
  {
    // Identifiers are escaped by repeating them
    $escapedIdentifier = $this->identifier.$this->identifier;

    if (is_array($value)) {
      list($value, $alias) = $value;
      $alias = str_replace($this->identifier, $escapedIdentifier, $alias);
    }

    if ($value instanceof DatabaseQuery) {
      // Create a sub-query
      $value = '('.$value->compile($this).')';
    } elseif ($value instanceof DatabaseExpression) {
      // Compile the expression
      $value = $value->compile($this);
    } else {
      // Convert to a string
      $value = (string) $value;
      $value = str_replace($this->identifier, $escapedIdentifier, $value);

      if (strpos($value, '.') !== false) {
        $parts = explode('.', $value);

        foreach ($parts as & $part) {
          // Quote each of the parts
          $part = $this->identifier.$part.$this->identifier;
        }

        $value = implode('.', $parts);
      } else {
        $value = $this->identifier.$value.$this->identifier;
      }
    }

    if (isset($alias)) {
      $value .= ' AS '.$this->identifier.$alias.$this->identifier;
    }

    return $value;
  }

  /**
   * Sanitize a string by escaping characters that could cause an SQL
   * injection attack.
   *
   *     $value = $db->escape('any string');
   *
   * @param   string   $value  value to quote
   * @return  string
   */
  abstract public function escape($value);
  
  /**
   * Returns a temporary location to cache queries
   * 
   * @param string $name Name of the instance
   * @return string
   */
  public function queryCacheTempPath($name)
  {
    $f = tempnam(sys_get_temp_dir(), 'TMP');
    unlink($f);
    $tmp = basename($f);
    $id = md5(__FILE__ . $name);
    
    return "{$tmp}/{$id}";
  }
  
  /**
   * Gets/sets query cache path
   * 
   * @param string $path Path to cache folder
   * @return string|$this
   */
  public function queryCachePath($path = null)
  {
    if ($path === null) {
      return $this->queryCachePath;
    }
    
    $this->queryCachePath = (string)$path;
    
    return $this;
  }
  
  /**
   * Gets/sets query cache path
   * 
   * @param string $path Path to cache folder
   * @return string|$this
   */
  public function queryCacheLifetime($lifetime = null)
  {
    if ($lifetime === null) {
      return $this->queryCacheLifetime;
    }
    
    $this->queryCacheLifetime = (int)$lifetime;
    
    return $this;
  }
  
  /**
   * Caches queries in filesystem
   * 
   * @param   string  $key        key of the cache
   * @param   mixed   $data       data to cache
   * @param   integer $lifetime   number of seconds the cache is valid for
   * @return  mixed    for getting
   * @return  boolean  for setting
   */
  public function cacheQuery($key, $data = null, $lifetime = null)
  {
    // Cache file is a hash of the name
    $file = sha1($key).'.txt';
  
    // Cache directories are split by keys to prevent filesystem overload
    $dir = $this->queryCachePath.DIRECTORY_SEPARATOR.$file[0].$file[1].DIRECTORY_SEPARATOR;
  
    if ($lifetime === null) {
      // Use the default lifetime
      $lifetime = $this->queryCacheLifetime ?: 0;
    }
  
    if ($data === null) {
      if (is_file($dir.$file)) {
        if ((time() - filemtime($dir.$file)) < $lifetime) {
          // Return the cache
          try {
            return unserialize(file_get_contents($dir.$file));
          } catch (Exception $e) {
            // Cache is corrupt
          }
        } else {
          try {
            // Cache has expired
            unlink($dir.$file);
          } catch (Exception $e) {
            // Cache has mostly likely already been deleted,
          }
        }
      }
      // Cache not found
      return null;
    }
  
    if (!is_dir($dir)) {
      // Create the cache directory
      mkdir($dir, 0777, true);
      // Set permissions (must be manually set to fix umask issues)
      chmod($dir, 0777);
    }
  
    // Force the data to be a string
    $data = serialize($data);
  
    try {
      // Write the cache
      return (bool)file_put_contents($dir.$file, $data, LOCK_EX);
    } catch (Exception $e) {
      // Failed to write cache
      return false;
    }
  }
  
  /**
   * Caches querie if a cache system was configured, or fallback to filesystem
   * 
   * @param   string  $key        key of the cache
   * @param   mixed   $data       data to cache
   * @param   integer $lifetime   number of seconds the cache is valid for
   * @return  mixed    for getting
   * @return  $this    for setting
   */
  public function cache($key, $data = null, $lifetime = null)
  {
    if ($this->cache !== null) {
      if ($data === null) {
        return Arr::get($this->cache, $key);
      }
      
      $this->cache[$key] = $value;
    } else {
      $this->cacheQuery($key, $data, $lifetime);
    }
    
    return $this;
  }
}
