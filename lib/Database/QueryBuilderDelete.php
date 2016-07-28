<?php

namespace KC\Database;

/**
 * Database query builder for DELETE statements.
 */
class QueryBuilderDelete extends QueryBuilderWhere
{
  // DELETE FROM ...
  protected $table;
  
  /**
   * Set the table for a delete.
   *
   * @param   mixed  $table  table name or array($table, $alias) or object
   * @return  void
   */
  public function __construct($table = null)
  {
    if ($table) {
      // Set the inital table name
      $this->table = $table;
    }
    // Start the query with no SQL
    return parent::__construct(Database::DELETE, '');
  }
  
  /**
   * Sets the table to delete from.
   *
   * @param   mixed  $table  table name or array($table, $alias) or object
   * @return  $this
   */
  public function table($table)
  {
    $this->table = $table;
  
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
    // Start a deletion query
    $query = 'DELETE FROM '.$db->quoteTable($this->table);
  
    if (!empty($this->where)) {
      // Add deletion conditions
      $query .= ' WHERE '.$this->compileConditions($db, $this->where);
    }
  
    if (!empty($this->orderBy)) {
      // Add sorting
      $query .= ' '.$this->compileOrderBy($db, $this->orderBy);
    }
  
    if ($this->limit !== null)
    {
      // Add limiting
      $query .= ' LIMIT '.$this->limit;
    }
  
    $this->sql = $query;
  
    return parent::compile($db);
  }
  
  /**
   * Reset the current builder status.
   *
   * @return  $this
   */
  public function reset()
  {
    $this->table = null;
    $this->where = [];
    $this->parameters = [];
    $this->sql = null;
  
    return $this;
  }
}
