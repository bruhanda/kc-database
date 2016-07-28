<?php

namespace KC\Database;

/**
 * Database expressions can be used to add unescaped SQL fragments to a
 * [QueryBuilder] object.
 *
 * For example, you can use an expression to generate a column alias:
 *
 *     // SELECT CONCAT(first_name, last_name) AS full_name
 *     $query = DB::select(array(DB::expr('CONCAT(first_name, last_name)'), 'full_name')));
 */
class DatabaseExpression {

  // Unquoted parameters
  protected $parameters;

  // Raw expression string
  protected $value;

  /**
   * Sets the expression string.
   *
   *     $expression = new DatabaseExpression('COUNT(users.id)');
   *
   * @param   string  $value      raw SQL expression string
   * @param   array   $parameters unquoted parameter values
   * @return  void
   */
  public function __construct($value, $parameters = [])
  {
    // Set the expression string
    $this->value = $value;
    $this->parameters = $parameters;
  }

  /**
   * Bind a variable to a parameter.
   *
   * @param   string  $param  parameter key to replace
   * @param   mixed   $var    variable to use
   * @return  $this
   */
  public function bind($param, & $var)
  {
    $this->parameters[$param] =& $var;

    return $this;
  }

  /**
   * Set the value of a parameter.
   *
   * @param   string  $param  parameter key to replace
   * @param   mixed   $value  value to use
   * @return  $this
   */
  public function param($param, $value)
  {
    $this->parameters[$param] = $value;

    return $this;
  }

  /**
   * Add multiple parameter values.
   *
   * @param   array   $params list of parameter values
   * @return  $this
   */
  public function parameters(array $params)
  {
    $this->parameters = $params + $this->parameters;

    return $this;
  }

  /**
   * Get the expression value as a string.
   *
   *     $sql = $expression->value();
   *
   * @return  string
   */
  public function value()
  {
    return (string)$this->value;
  }

  /**
   * Return the value of the expression as a string.
   *
   *     echo $expression;
   *
   * @return  string
   * @uses    Database_Expression::value
   */
  public function __toString()
  {
    return $this->value();
  }

  /**
   * Compile the SQL expression and return it. Replaces any parameters with
   * their given values.
   *
   * @param   Database    Database instance
   * @return  string
   */
  public function compile(Database $db)
  {
    $value = $this->value();

    if (!empty($this->parameters)) {
      // Quote all of the parameter values
      $params = array_map(array($db, 'quote'), $this->parameters);
      // Replace the values in the expression
      $value = strtr($value, $params);
    }

    return $value;
  }
}
