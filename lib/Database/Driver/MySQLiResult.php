<?php 

namespace KC\Database\Driver;

use KC\Database\Database;
use KC\Database\DatabaseResult;
use KC\Database\DatabaseException;

/**
 * MySQLi driver database result
 */
class MySQLiResult extends DatabaseResult
{
  protected $internalRow = 0;
  
  public function __construct($result, $sql, $asObject = false, array $params = null)
  {
    parent::__construct($result, $sql, $asObject, $params);
  
    // Find the number of rows in the result
    $this->totalRows = $result->num_rows;
  }
  
  public function __destruct()
  {
    if (is_resource($this->result)) {
      $this->result->free();
    }
    
    $this->result = null;
  }
  
  public function seek($offset)
  {
    if ($this->offsetExists($offset) && $this->result->data_seek($offset)) {
      // Set the current row to the offset
      $this->currentRow = $this->internalRow = $offset;
      return true;
    } 
    
    return false;
  }
  
  public function current()
  {
    if ($this->currentRow !== $this->internalRow && !$this->seek($this->currentRow)) {
      return null;
    }
    // Increment internal row for optimization assuming rows are fetched in order
    $this->internalRow++;
  
    if ($this->asObject === true) {
      // Return an stdClass
      return $this->result->fetch_object();
    } elseif (is_string($this->asObject)) {
      // Return an object of given class name
      return $this->result->fetch_object($this->asObject, (array)$this->objectParams);
    } else {
      // Return an array of the row
      return $this->result->fetch_assoc();
    }
  }
}
