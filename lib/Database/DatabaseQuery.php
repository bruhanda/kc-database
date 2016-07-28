<?php

namespace KC\Database;

/**
 * Database query wrapper.
 */
class DatabaseQuery
{
  // Query type
  protected $type;
  
  // Execute the query during a cache hit
  protected $forceExecute = false;
  
  // Cache lifetime
  protected $lifetime = null;
  
  // SQL statement
  protected $sql;
  
  // Quoted query parameters
  protected $parameters = [];
  
  // Return results as associative arrays or objects
  protected $asObject = false;
  
  // Parameters for __construct when using object results
  protected $objectParams = [];
  
  /**
   * Creates a new SQL query of the specified type.
   *
   * @param   integer  $type  query type: Database::SELECT, Database::INSERT, etc
   * @param   string   $sql   query string
   * @return  void
   */
  public function __construct($type, $sql)
  {
    $this->type = $type;
    $this->sql = $sql;
  }
  
  /**
   * Get the type of the query.
   *
   * @return  integer
   */
  public function type()
  {
    return $this->type;
  }
  
  /**
   * Enables the query to be cached for a specified amount of time.
   *
   * @param   integer  $lifetime  number of seconds to cache, 0 deletes it from the cache,
   *                              boolean true uses the db driver cache lifetime setting
   * @param   boolean  $force     whether or not to execute the query during a cache hit
   * @return  $this
   */
  public function cached($lifetime = true, $force = false)
  {
    $this->forceExecute = $force;
    $this->lifetime = $lifetime;
  
    return $this;
  }
  
  /**
   * Returns results as associative arrays
   *
   * @return  $this
   */
  public function asAssoc()
  {
    $this->asObject = false;
    $this->objectParams = [];
  
    return $this;
  }
  
  /**
   * Returns results as objects
   *
   * @param   string  $class  classname or true for stdClass
   * @param   array   $params
   * @return  $this
   */
  public function asObject($class = true, array $params = null)
  {
    $this->asObject = $class;
  
    if ($params) {
      // Add object parameters
      $this->objectParams = $params;
    }
  
    return $this;
  }
  
  /**
   * Set the value of a parameter in the query.
   *
   * @param   string   $param  parameter key to replace
   * @param   mixed    $value  value to use
   * @return  $this
   */
  public function param($param, $value)
  {
    // Add or overload a new parameter
    $this->parameters[$param] = $value;
  
    return $this;
  }
  
  /**
   * Bind a variable to a parameter in the query.
   *
   * @param   string  $param  parameter key to replace
   * @param   mixed   $var    variable to use
   * @return  $this
   */
  public function bind($param, & $var)
  {
    // Bind a value to a variable
    $this->parameters[$param] =& $var;
  
    return $this;
  }
  
  /**
   * Add multiple parameters to the query.
   *
   * @param   array  $params  list of parameters
   * @return  $this
   */
  public function parameters(array $params)
  {
    // Merge the new parameters in
    $this->parameters = $params + $this->parameters;
  
    return $this;
  }
  
  /**
   * Compile the SQL query and return it. Replaces any parameters with their
   * given values.
   *
   * @param   mixed  $db  Database instance or name of instance
   * @return  string
   */
  public function compile(Database $db)
  {
    // Import the SQL locally
    $sql = $this->sql;
  
    if (!empty($this->parameters)) {
      // Quote all of the values
      $values = array_map(array($db, 'quote'), $this->parameters);
      // Replace the values in the SQL
      $sql = strtr($sql, $values);
    }
  
    return $sql;
  }
  
  /**
   * Execute the current query on the given database.
   *
   * @param   mixed    $db  Database instance or name of instance
   * @param   string   result object classname, true for stdClass or false for array
   * @param   array    result object constructor arguments
   * @return  object   Database_Result for SELECT queries
   * @return  mixed    the insert id for INSERT queries
   * @return  integer  number of affected rows for all other queries
   */
  public function execute(Database $db, $asObject = null, $objectParams = null)
  {
    if ($asObject === null) {
      $asObject = $this->asObject;
    }
  
    if ($objectParams === null) {
      $objectParams = $this->objectParams;
    }
  
    // Compile the SQL query
    $sql = $this->compile($db);
    $lifetime = $this->lifetime;
    
    if ($this->lifetime === true) {
      $lifetime = $db->queryCacheLifetime();
    }
  
    if ($lifetime !== null && $this->type === Database::SELECT)
    {
      // Set the cache key based on the database instance name and SQL
      $cacheKey = 'Database::query("'.$db->name().'", "'.$sql.'")';
  
      // Read the cache first to delete a possible hit with lifetime <= 0
      if (($result = $db->cache($cacheKey, null, $lifetime)) !== null && !$this->forceExecute) {
        // Return a cached result
        return new DatabaseResultCached($result, $sql, $asObject, $objectParams);
      }
    }
  
    // Execute the query
    $result = $db->query($this->type, $sql, $asObject, $objectParams);
  
    if (isset($cacheKey) && $lifetime > 0) {
      // Cache the result array
      $db->cache($cacheKey, $result->asArray(), $lifetime);
    }
  
    return $result;
  }
}

