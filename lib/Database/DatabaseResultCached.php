<?php

namespace KC\Database;

/**
 * Object used for caching the results of select queries.
 */
class DatabaseResultCached extends DatabaseResult
{
  public function __construct(array $result, $sql, $asObject = null)
  {
    parent::__construct($result, $sql, $asObject);
    // Find the number of rows in the result
    $this->totalRows = count($result);
  }
  
  public function cached()
  {
    return $this;
  }
  
  public function seek($offset)
  {
    if ($this->offsetExists($offset)) {
      $this->currentRow = $offset;
      return true;
    } 
    
    return false;
  }
  
  public function current()
  {
    // Return an array of the row
    return $this->valid() ? $this->result[$this->currentRow] : null;
  }
}
