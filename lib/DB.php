<?php

namespace KC;

use KC\Database\DatabaseQuery;
use KC\Database\QueryBuilderSelect;
use KC\Database\QueryBuilderInsert;
use KC\Database\QueryBuilderUpdate;
use KC\Database\QueryBuilderDelete;
use KC\Database\DatabaseExpression;

/**
 * Provides a shortcut to get Database related objects for [making queries](../database/query).
 *
 * Shortcut     | Returned Object
 * -------------|---------------
 * [`DB::query()`](#query)   | [DatabaseQuery]
 * [`DB::insert()`](#insert) | [QueryBuilderInsert]
 * [`DB::select()`](#select),<br />[`DB::select_array()`](#select_array) | [QueryBuilderSelect]
 * [`DB::update()`](#update) | [QueryBuilderUpdate]
 * [`DB::delete()`](#delete) | [QueryBuilderDelete]
 * [`DB::expr()`](#expr)     | [DatabaseExpression]
 *
 * You pass the same parameters to these functions as you pass to the objects they return.
 */
class DB {

	/**
	 * Create a new [DatabaseQuery] of the given type.
	 *
	 *     // Create a new SELECT query
	 *     $query = DB::query(DatabaseConnection::SELECT, 'SELECT * FROM users');
	 *
	 *     // Create a new DELETE query
	 *     $query = DB::query(DatabaseConnection::DELETE, 'DELETE FROM users WHERE id = 5');
	 *
	 * Specifying the type changes the returned result. When using
	 * `DatabaseConnection::SELECT`, a [DatabaseResult] will be returned.
	 * `DatabaseConnection::INSERT` queries will return the insert id and number of rows.
	 * For all other queries, the number of affected rows is returned.
	 *
	 * @param   integer  $type  type: DatabaseConnection::SELECT, etc
	 * @param   string   $sql   SQL statement
	 * @return  Database_Query
	 */
	public static function query($type, $sql)
	{
		return new DatabaseQuery($type, $sql);
	}

	/**
	 * Create a new [QueryBuilderSelect]. Each argument will be
	 * treated as a column. To generate a `foo AS bar` alias, use an array.
	 *
	 *     // SELECT id, username
	 *     $query = DB::select('id', 'username');
	 *
	 *     // SELECT id AS user_id
	 *     $query = DB::select(['id', 'user_id']);
	 *
	 * @param   mixed   $columns  column name or array($column, $alias) or object
	 * @return  QueryBuilderSelect
	 */
	public static function select($columns = null)
	{
		return new QueryBuilderSelect(func_get_args());
	}

	/**
	 * Create a new [QueryBuilderSelect] from an array of columns.
	 *
	 *     // SELECT id, username
	 *     $query = DB::selectArray(array('id', 'username'));
	 *
	 * @param   array   $columns  columns to select
	 * @return  QueryBuilderSelect
	 */
	public static function selectArray(array $columns = null)
	{
		return new QueryBuilderSelect($columns);
	}

	/**
	 * Create a new [QueryBuilderInsert].
	 *
	 *     // INSERT INTO users (id, username)
	 *     $query = DB::insert('users', array('id', 'username'));
	 *
	 * @param   string  $table    table to insert into
	 * @param   array   $columns  list of column names or array($column, $alias) or object
	 * @return  QueryBuilderInsert
	 */
	public static function insert($table = null, array $columns = null)
	{
		return new QueryBuilderInsert($table, $columns);
	}

	/**
	 * Create a new [QueryBuilderUpdate].
	 *
	 *     // UPDATE users
	 *     $query = DB::update('users');
	 *
	 * @param   string  $table  table to update
	 * @return  QueryBuilderUpdate
	 */
	public static function update($table = null)
	{
		return new QueryBuilderUpdate($table);
	}

	/**
	 * Create a new [QueryBuilderDelete].
	 *
	 *     // DELETE FROM users
	 *     $query = DB::delete('users');
	 *
	 * @param   string  $table  table to delete from
	 * @return  QueryBuilderDelete
	 */
	public static function delete($table = null)
	{
		return new QueryBuilderDelete($table);
	}

	/**
	 * Create a new [DatabaseExpression] which is not escaped. An expression
	 * is the only way to use SQL functions within query builders.
	 *
	 *     $expression = DB::expr('COUNT(users.id)');
	 *     $query = DB::update('users')->set(array('login_count' => DB::expr('login_count + 1')))->where('id', '=', $id);
	 *
	 * @param   string  $string  expression
	 * @param   array   parameters
	 * @return  DatabaseExpression
	 */
	public static function expr($string, $parameters = [])
	{
		return new DatabaseExpression($string, $parameters);
	}

}
