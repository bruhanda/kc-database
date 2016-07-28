<?php

namespace KC;

use KC\Database\DatabaseException;
use KC\Database\Arr;
use KC\Database\Database as DatabaseDriver;
use KC\Database\QueryTypeAwareInterface;

/**
 * Static factory
 */
class Database implements QueryTypeAwareInterface
{
  // Configurable driver classes for the static factory
  public static $defaultDrivers = [
    'mysqli' => "KC\\Database\\Driver\\MySQLiDriver"
  ];
  
  /**
   * Creates a connection object reading the driver from config.
   * 
   * A name is mandatory for a connection. This must be supplied either as
   * string first argument, or in the config. If the config contains the connection
   * name, second argument can be ignored, and the config can be passed as first argument.
   * 
   * @param   string|array|ArrayAccess  $name   Name of connection or configuration that contains a name
   * @param   array|ArrayAccess         $config Configuration object or array if not supplied as first argument
   * @return  Database                          A database connetion driver instance
   * @throws  DatabaseException
   */
  public static function instance($name, $config = null)
  {
    if (Arr::like($name)) {
      $config = $name;
      $name = Arr::get($config, 'name');
    }
    
    if (!Arr::like($config)) {
      throw new DatabaseException("The \$config parameter must be an array or ArrayAccess implementation");
    }
    if (empty($name)) {
      throw new DatabaseException('A connection must have a connection "name" key configured');
    }
    
    $driver = Arr::get($config, 'driver');
    
    try {    
      if (is_string($driver) && array_key_exists($driver, static::$defaultDrivers)) {
        // expect a default driver as string
        $instance = new static::$defaultDrivers[$driver]($name, $config);
      } elseif (is_string($driver)) {
        // or expect a class FQN as string
        $instance = new $driver($name, $config);
      } elseif (is_callable($driver)) {
        // or expect a factory
        $instance = call_user_func($driver, $name, $config);
      } else {
        // or fail
        throw new DatabaseException('Invalid configuration option for "driver" key');
      }
    } catch (DatabaseException $e) {
      throw $e;
    } catch (Exception $e) {
      throw new DatabaseException("Connection failed. Reason: {$e->getMessage()}");
    }
    
    if (!($instance instanceof DatabaseDriver)) {
      $class = get_class($connection);
      throw new DatabaseException("Unexpected returned object from driver. Expected [KC\Database\Database] but got [{$class}]");
    }
    
    return $instance;
  }
}
