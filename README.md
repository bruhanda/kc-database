# KC-Database

## What is it ?

Kohana Framework's DBAL (database module) as a decoupled component.

## Eg ?


```php
use KC\Database;
use KC\DB;

$db = Database::instance("default", [
  "driver" => "mysqli",
  "connection" => [
    "database" => "somedb",
    "username" => "root",
    "password" => "somepass"
  ]
]);


$result = DB::select('u.id', 'u.email', [DB::expr('group_concat(r.name)'), 'roles'])
  ->from(['users', 'u'])
      ->join(['roles_users', 'ru'])
        ->on('u.id', '=', 'ru.user_id')
      ->join(['roles', 'r'])
        ->on('r.id', '=', 'ru.role_id')
  ->groupBy('u.id')
  ->execute($db)
  ->asArray();

var_dump($result);

```

Outputs

```
array(1) {
  [0]=>
  array(3) {
    ["id"]=>
    string(2) "18"
    ["email"]=>
    string(16) "something@something.some"
    ["roles"]=>
    string(11) "login,admin"
  }
}
```

## More Info ?

@TBD

Since the Kohana components are hard coupled to the framework, there are of course some inconsitencies.
I'll fill in this section with what exactly has changed in the following weeks
