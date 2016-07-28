<?php

namespace KC\Database;

/**
 * Database query builder for INSERT statements.
 */
class QueryBuilderInsert extends QueryBuilder
{
  // INSERT INTO ...
  protected $table;
  
  // (...)
  protected $columns = [];
  
  // VALUES (...)
  protected $values = [];
  
  /**
   * Set the table and columns for an insert.
   *
   * @param   mixed  $table    table name or array($table, $alias) or object
   * @param   array  $columns  column names
   * @return  void
   */
  public function __construct($table = null, array $columns = null)
  {
    if (is_array($table) && $columns === null) {
      list($columns, $table) = [$table, null];
    }
    if ($table) {
      // Set the inital table name
      $this->table($table);
    }
    if ($columns) {
      // Set the column names
      $this->columns = $columns;
    }
    // Start the query with no SQL
    return parent::__construct(Database::INSERT, '');
  }
  
  /**
   * Sets the table to insert into.
   *
   * @param   string  $table  table name
   * @return  $this
   */
  public function table($table)
  {
    if (!is_string($table)) {
      throw new DatabaseException('INSERT INTO syntax does not allow table aliasing');
    }
  
    $this->table = $table;
  
    return $this;
  }
  
  /**
   * Proxy for ::table
   */
  public function into($table)
  {
    return $this->table($table);
  }
  
  /**
   * Set the columns that will be inserted.
   *
   * @param   array  $columns  column names
   * @return  $this
   */
  public function columns(array $columns)
  {
    $this->columns = $columns;
  
    return $this;
  }
  
  /**
   * Adds or overwrites values. Multiple value sets can be added.
   *
   * @param   array   $values  values list
   * @param   ...
   * @return  $this
   */
  public function values(array $values)
  {
    if (!is_array($this->values)) {
      throw new DatabaseException('INSERT INTO ... SELECT statements cannot be combined with INSERT INTO ... VALUES');
    }
  
    // Get all of the passed values
    $values = func_get_args();
    
    foreach ($values as $value) {
      $this->values[] = $value;
    }
  
    return $this;
  }
  
  /**
   * Use a sub-query to for the inserted values.
   *
   * @param   object  $query  Database_Query of SELECT type
   * @return  $this
   */
  public function select(DatabaseQuery $query)
  {
    if ($query->type() !== Database::SELECT) {
      throw new DatabaseException('Only SELECT queries can be combined with INSERT queries');
    }
  
    $this->values = $query;
  
    return $this;
  }
  
  /**
   * Compile the SQL query and return it.
   *
   * @param   mixed  $db  Database instance or name of instance
   * @return  string
   */
  public function compile(Database $db)
  {
    // Start an insertion query
    $query = 'INSERT INTO '.$db->quoteTable($this->table);
  
    // Add the column names
    $query .= ' ('.implode(', ', array_map(array($db, 'quoteColumn'), $this->columns)).') ';
  
    if (is_array($this->values)) {
      // Callback for quoting values
      $quote = array($db, 'quote');
  
      $groups = [];
      foreach ($this->values as $group) {
        foreach ($group as $offset => $value) {
          if ((is_string($value) && array_key_exists($value, $this->parameters)) === false) {
            // Quote the value, it is not a parameter
            $group[$offset] = $db->quote($value);
          }
        }
        
        $groups[] = '('.implode(', ', $group).')';
      }
  
      // Add the values
      $query .= 'VALUES '.implode(', ', $groups);
    } else {
      // Add the sub-query
      $query .= $this->values->compile($db);
    }
  
    $this->sql = $query;
  
    return parent::compile($db);
  }
  
  public function reset()
  {
    $this->_table = null;
    $this->_columns = [];
    $this->_values  = [];
    $this->_parameters = [];
    $this->_sql = null;
  
    return $this;
  }
}
