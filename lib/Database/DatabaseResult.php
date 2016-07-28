<?php

namespace KC\Database;

use Countable;
use Iterator;
use SeekableIterator;
use ArrayAccess;

/**
 * Database result wrapper.
 */
abstract class DatabaseResult implements Countable, Iterator, SeekableIterator, ArrayAccess
{
  // Executed SQL for this result
  protected $query;
  
  // Raw result resource
  protected $result;
  
  // Total number of rows and current row
  protected $totalRows  = 0;
  protected $currentRow = 0;
  
  // Return rows as an object or associative array
  protected $asObject;
  
  // Parameters for __construct when using object results
  protected $objectParams = null;
  
  /**
   * Sets the total number of rows and stores the result locally.
   *
   * @param   mixed   $result     query result
   * @param   string  $sql        SQL query
   * @param   mixed   $asObject
   * @param   array   $params
   * @return  void
   */
  public function __construct($result, $sql, $asObject = false, array $params = null)
  {
    // Store the result locally
    $this->result = $result;
    // Store the SQL locally
    $this->query = $sql;
  
    if (is_object($asObject)){
      // Get the object class name
      $asObject = get_class($asObject);
    }
    // Results as objects or associative arrays
    $this->asObject = $asObject;
  
    if ($params) {
      // Object constructor params
      $this->objectParams = $params;
    }
  }
  
  /**
   * Result destruction cleans up all open result sets.
   *
   * @return  void
   */
  abstract public function __destruct();
  
  /**
   * Get a cached database result from the current result iterator.
   *
   *     $cachable = serialize($result->cached());
   *
   * @return  DatabaseResultCached
   * @since   3.0.5
   */
  public function cached()
  {
    return new DatabaseResultCached($this->asArray(), $this->query, $this->asObject);
  }
  
  /**
   * Return all of the rows in the result as an array.
   *
   *     // Indexed array of all rows
   *     $rows = $result->asArray();
   *
   *     // Associative array of rows by "id"
   *     $rows = $result->asArray('id');
   *
   *     // Associative array of rows, "id" => "name"
   *     $rows = $result->asArray('id', 'name');
   *
   * @param   string  $key    column for associative keys
   * @param   string  $value  column for values
   * @return  array
   */
  public function asArray($key = null, $value = null)
  {
    $results = array();
  
    if ($key === null && $value === null) {
      // Indexed rows
      foreach ($this as $row) {
        $results[] = $row;
      }
    } elseif ($key === null) {
      // Indexed columns
      if ($this->asObject) {
        foreach ($this as $row) {
          $results[] = $row->$value;
        }
      } else {
        foreach ($this as $row) {
          $results[] = $row[$value];
        }
      }
    } elseif ($value === null) {
      // Associative rows
      if ($this->asObject) {
        foreach ($this as $row) {
          $results[$row->$key] = $row;
        }
      } else {
        foreach ($this as $row) {
          $results[$row[$key]] = $row;
        }
      }
    } else {
      // Associative columns
      if ($this->asObject) {
        foreach ($this as $row) {
          $results[$row->$key] = $row->$value;
        }
      } else {
        foreach ($this as $row) {
          $results[$row[$key]] = $row[$value];
        }
      }
    }
  
    $this->rewind();
  
    return $results;
  }
  
  /**
   * Return the named column from the current row.
   *
   *     // Get the "id" value
   *     $id = $result->get('id');
   *
   * @param   string  $name     column to get
   * @param   mixed   $default  default value if the column does not exist
   * @return  mixed
   */
  public function get($name, $default = null)
  {
    $row = $this->current();
  
    if ($this->asObject) {
      if (isset($row->$name)) {
        return $row->$name;
      }
    } else {
      if (isset($row[$name])) {
        return $row[$name];
      }
    }
  
    return $default;
  }
  
  /**
   * Implements [Countable::count], returns the total number of rows.
   *
   *     echo count($result);
   *
   * @return  integer
   */
  public function count()
  {
    return $this->totalRows;
  }
  
  /**
   * Implements [ArrayAccess::offsetExists], determines if row exists.
   *
   *     if (isset($result[10])) {
   *         // Row 10 exists
   *     }
   *
   * @param   int     $offset
   * @return  boolean
   */
  public function offsetExists($offset)
  {
    return ($offset >= 0 && $offset < $this->totalRows);
  }
  
  /**
   * Implements [ArrayAccess::offsetGet], gets a given row.
   *
   *     $row = $result[10];
   *
   * @param   int     $offset
   * @return  mixed
   */
  public function offsetGet($offset)
  {
    if (!$this->seek($offset)) {
      return null;
    }
  
    return $this->current();
  }
  
  /**
   * Implements [ArrayAccess::offsetSet], throws an error.
   *
   * [!!] You cannot modify a database result.
   *
   * @param   int     $offset
   * @param   mixed   $value
   * @return  void
   * @throws  DatabaseException
   */
  final public function offsetSet($offset, $value)
  {
    throw new DatabaseException('Database results are read-only');
  }
  
  /**
   * Implements [ArrayAccess::offsetUnset], throws an error.
   *
   * [!!] You cannot modify a database result.
   *
   * @param   int     $offset
   * @return  void
   * @throws  DatabaseException
   */
  final public function offsetUnset($offset)
  {
    throw new DatabaseException('Database results are read-only');
  }
  
  /**
   * Implements [Iterator::key], returns the current row number.
   *
   *     echo key($result);
   *
   * @return  integer
   */
  public function key()
  {
    return $this->currentRow;
  }
  
  /**
   * Implements [Iterator::next], moves to the next row.
   *
   *     next($result);
   *
   * @return  $this
   */
  public function next()
  {
    ++$this->currentRow;
    return $this;
  }
  
  /**
   * Implements [Iterator::prev], moves to the previous row.
   *
   *     prev($result);
   *
   * @return  $this
   */
  public function prev()
  {
    --$this->currentRow;
    return $this;
  }
  
  /**
   * Implements [Iterator::rewind], sets the current row to zero.
   *
   *     rewind($result);
   *
   * @return  $this
   */
  public function rewind()
  {
    $this->currentRow = 0;
    return $this;
  }
  
  /**
   * Implements [Iterator::valid], checks if the current row exists.
   *
   * [!!] This method is only used internally.
   *
   * @return  boolean
   */
  public function valid()
  {
    return $this->offsetExists($this->currentRow);
  }
}
