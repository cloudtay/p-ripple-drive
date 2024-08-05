<?php declare(strict_types=1);

namespace Psc\Drive\Laravel\Coroutine;

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlConnectionPool;
use Amp\Mysql\MysqlTransaction;
use Closure;
use Exception;
use Fiber;
use Illuminate\Database\Connection;
use Illuminate\Database\Connectors\ConnectionFactory;
use Illuminate\Database\MariaDbConnection;
use Illuminate\Database\MySqlConnection;
use Illuminate\Database\PostgresConnection;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Database\SqlServerConnection;
use Illuminate\Foundation\Application;
use InvalidArgumentException;
use PDO;
use Throwable;

use function spl_object_hash;

class MySQL
{
    /**
     * @param Application $app
     * @return ConnectionFactory
     */
    public function getFactory(Application $app): ConnectionFactory
    {
        return new class ($app) extends ConnectionFactory {
            /**
             * Create a new connection instance.
             *
             * @param string      $driver
             * @param PDO|Closure $connection
             * @param string      $database
             * @param string      $prefix
             * @param array       $config
             * @return SQLiteConnection|MariaDbConnection|MySqlConnection|PostgresConnection|SqlServerConnection|Connection
             *
             */
            protected function createConnection($driver, $connection, $database, $prefix = '', array $config = []): SQLiteConnection|MariaDbConnection|MySqlConnection|PostgresConnection|SqlServerConnection|Connection
            {
                return match ($driver) {
                    'mysql' => $this->getConnection($connection, $database, $prefix, $config),
                    'mariadb' => new MariaDbConnection($connection, $database, $prefix, $config),
                    'pgsql' => new PostgresConnection($connection, $database, $prefix, $config),
                    'sqlite' => new SQLiteConnection($connection, $database, $prefix, $config),
                    'sqlsrv' => new SqlServerConnection($connection, $database, $prefix, $config),
                    default => throw new InvalidArgumentException("Unsupported driver [{$driver}]."),
                };
            }

            /**
             * @param $connection
             * @param $database
             * @param $prefix
             * @param $config
             * @return Connection
             */
            private function getConnection($connection, $database, $prefix, $config): Connection
            {
                return new class ($connection, $database, $prefix, $config) extends MySqlConnection {
                    /*** @var MysqlConnectionPool */
                    private MysqlConnectionPool $pool;

                    /**
                     * @param        $pdo
                     * @param string $database
                     * @param string $tablePrefix
                     * @param array  $config
                     */
                    public function __construct($pdo, string $database = '', string $tablePrefix = '', array $config = [])
                    {
                        parent::__construct($pdo, $database, $tablePrefix, $config);
                        $config     = MysqlConfig::fromString(
                            "host=localhost user=root password=aa123456 db=information_schema"
                        );
                        $this->pool = new MysqlConnectionPool($config);
                        unset($this->pdo);
                    }

                    /**
                     * @param string $query
                     * @param array  $bindings
                     * @param bool   $useReadPdo
                     * @return mixed
                     */
                    public function select($query, $bindings = [], $useReadPdo = true): mixed
                    {
                        return $this->run($query, $bindings, function ($query, $bindings) use ($useReadPdo) {
                            if ($this->pretending()) {
                                return [];
                            }

                            return $this->statement($query, $bindings);
                        });
                    }

                    /**
                     * @param string $query
                     * @param array  $bindings
                     * @return mixed
                     */
                    public function statement($query, $bindings = []): mixed
                    {
                        return $this->run($query, $bindings, function ($query, $bindings) {
                            if ($this->pretending()) {
                                return [];
                            }

                            $statement = $this->getTransaction()?->prepare($query) ?? $this->pool->prepare($query);
                            return $statement->execute($this->prepareBindings($bindings));
                        });
                    }

                    /**
                     * @return void
                     */
                    public function beginTransaction(): void
                    {
                        $transaction = $this->pool->beginTransaction();
                        ;
                        if ($fiber = Fiber::getCurrent()) {
                            $this->fiber2transaction[spl_object_hash($fiber)] = $transaction;
                        } else {
                            $this->fiber2transaction['main'] = $transaction;
                        }
                    }

                    /**
                     * @return void
                     */
                    public function commit(): void
                    {
                        if ($fiber = Fiber::getCurrent()) {
                            $key = spl_object_hash($fiber);
                        } else {
                            $key = 'main';
                        }

                        if (!$transaction = $this->fiber2transaction[$key] ?? null) {
                            throw new Exception('Transaction not found');
                        }

                        $transaction->commit();
                        unset($this->fiber2transaction[$key]);
                    }

                    /**
                     * @param $toLevel
                     * @return void
                     */
                    public function rollBack($toLevel = null): void
                    {
                        if ($fiber = Fiber::getCurrent()) {
                            $key = spl_object_hash($fiber);
                        } else {
                            $key = 'main';
                        }

                        if (!$transaction = $this->fiber2transaction[$key] ?? null) {
                            throw new Exception('Transaction not found');
                        }

                        $transaction->rollback();
                        unset($this->fiber2transaction[$key]);
                    }

                    /**
                     * @var MysqlTransaction[]
                     */
                    private array $fiber2transaction = [];

                    /**
                     * @param Closure $callback
                     * @param int     $attempts
                     * @return void
                     */
                    public function transaction(Closure $callback, $attempts = 1): void
                    {
                        $this->beginTransaction();
                        try {
                            $callback();
                            $this->commit();
                        } catch (Throwable $e) {
                            $this->rollBack();
                            throw $e;
                        }
                    }

                    /**
                     * @return MysqlTransaction|null
                     */
                    private function getTransaction(): MysqlTransaction|null
                    {
                        if ($fiber = Fiber::getCurrent()) {
                            $key = spl_object_hash($fiber);
                        } else {
                            $key = 'main';
                        }

                        if (!$transaction = $this->fiber2transaction[$key] ?? null) {
                            return null;
                        }

                        return $transaction;
                    }
                };
            }
        };
    }


}
