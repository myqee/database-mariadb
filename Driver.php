<?php
namespace MyQEE\Database\MySQLi;

use \MyQEE\Database\DriverSQL;
use \Exception;

/**
 * 数据库MySQLi返回类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MySQLi
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Driver extends DriverSQL
{
    /**
     * MySQL使用反引号标识符
     *
     * @var string
     */
    protected $identifier = '`';

    /**
     * 默认端口
     *
     * @var int
     */
    protected $defaultPort = 3306;

    /**
     * 引擎是MySQL
     *
     * @var bool
     */
    protected $isMySQL = true;


    /**
     * 连接数据库
     *
     * @param array $config
     * @return string 返回连接的id
     */
    public function doConnect(array $config)
    {
        try
        {
            if (empty($persistent))
            {
                $resource = \mysqli_init();

                \mysqli_options($resource, \MYSQLI_OPT_CONNECT_TIMEOUT, 3);

                if (isset($this->config['option']) && is_array($this->config['option']))
                {
                    foreach ($this->config['option'] as $k => $v)
                    {
                        \mysqli_options($resource, $k, $v);
                    }
                }

                \mysqli_real_connect($resource, $config['hostname'], $config['username'], $config['password'], $config['database'], $config['port'], null, \MYSQLI_CLIENT_COMPRESS);
            }
            else
            {
                $resource = new \mysqli($config['hostname'], $config['username'], $config['password'], $config['database'], $config['port']);
            }

            # 设置语言
            $resource->set_charset($this->config['charset']);

            return $resource;
        }
        catch (Exception $e)
        {
            if (2 === $e->getCode() && preg_match('#(Unknown database|Access denied for user)#i', $e->getMessage(), $m))
            {
                # 指定的库不存在，直接返回
                $lastError = strtolower($m[1]) === 'unknown database' ? __('The mysql database does not exist') : __('The mysql database account or password error');
            }
            else
            {
                $lastError = $e->getMessage();
            }

            $lastErrorCode = $e->getCode();
        }

        throw new Exception($lastError, $lastErrorCode);
    }

    /**
     * 检查连接（每5秒钟间隔才检测）
     *
     * @param $id
     * @param int $limit 时间间隔（秒）, 0 表示一直检查
     * @return bool
     */
    protected function checkConnect($id, $limit = 5)
    {
        $tmp = $this->connections[$id];

        if (0 === $limit || time() - $tmp['time'] > $limit)
        {
            if (\mysqli_ping($tmp['resource']))
            {
                return true;
            }
            else
            {
                # 自动移除失败的连接
                $this->release($id);

                return false;
            }
        }
        else
        {
            return true;
        }
    }

    public function closeConnect()
    {
        if ($this->connections)
        {
            foreach ($this->connections as $key => $connection)
            {
                $connection['resource']->close();

                if(INCLUDE_MYQEE_CORE && IS_DEBUG)Core::debug()->info('close '. $key .' connection.');
            }

            $this->connectionId = null;
            $this->connections  = [];
        }
    }

    /**
     * 切换表
     *
     * @param $database
     * @return bool|void
     * @throws Exception
     */
    public function selectDB($database)
    {
        if (!$database)return false;

        if (!$this->connectionId)return false;

        $connection = $this->connections[$this->connectionId];

        if ($connection['database'] !== $database)
        {
            if (\mysqli_select_db($connection['resource'], $database))
            {
                $this->connections[$this->connectionId]['database'] = $database;

                return true;
            }
            else
            {
                throw new Exception('选择数据表错误:' . \mysqli_error($connection['resource']), \mysqli_errno($connection['resource']));
            }
        }
        else
        {
            return false;
        }
    }

    public function escape($value)
    {
        if ($this->currentConnection)
        {
            # 直接使用当前连接
            $connection = $this->currentConnection;
        }
        else
        {
            $connection = $this->connection();
        }

        $this->convertEncoding($value);

        if (($value = \mysqli_real_escape_string($connection, $value)) === false)
        {
            throw new Exception('Error:' . \mysqli_errno($connection), \mysqli_error($connection));
        }

        return "'$value'";
    }

    /**
     * 查询
     *
     * $use_connection_type 默认不传为自动判断，可传true/false,若传字符串(只支持a-z0-9的字符串)，则可以切换到另外一个连接，比如传other,则可以连接到$this->_connection_other_id所对应的ID的连接
     *
     * @param string|\MyQEE\Database\QueryBuilder $sql 查询语句
     * @param string $asObject 是否返回对象
     * @param boolean $clusterName 是否使用主数据库，不设置则自动判断
     * @return Result
     */
    public function query($sql, $asObject = null, $clusterName = null)
    {
        if (is_object($sql) && $sql instanceof \MyQEE\Database\QueryBuilder)
        {
            if (null === $clusterName)
            {
                # select 查询, 默认使用 slave
                $clusterName = 'slave';
            }
            elseif (true === $clusterName)
            {
                # 设置了使用主库
                $clusterName = 'master';
            }

            # 由于解析sql语句时用到 mysqli_real_escape_string, 需要先连接到数据库上
            $connection = $this->connection($clusterName);

            # 生成SQL语句
            $sql = $this->compile($sql->getAndResetBuilder(), 'select');

            $sqlType = 'select';
        }
        else
        {
            $sql = trim($sql);
            list($sqlType, $needMaster) = $this->getQueryType($sql);

            if ($needMaster)
            {
                $clusterName = 'master';
            }

            /**
             * @var $connection \mysqli
             */
            $connection = $this->connection($clusterName);
        }

        # 记录调试
        if(INCLUDE_MYQEE_CORE && IS_DEBUG)
        {
            Core::debug()->info($sql, 'MySQL');

            static $isSqlDebug = null;

            if (null === $isSqlDebug)
            {
                $isSqlDebug = (bool)Core::debug()->profiler('sql')->isOpen();
            }

            if ($isSqlDebug)
            {
                $benchmark = Core::debug()->profiler('sql')->start('Database', $this->connectionId);
            }
        }

        static $isNoCache = null;

        if (null === $isNoCache) $isNoCache = INCLUDE_MYQEE_CORE && IS_DEBUG ? (bool)Core::debug()->profiler('nocached')->is_open() : false;

        //显示无缓存数据
        if ($isNoCache && strtoupper(substr($sql, 0, 6)) === 'SELECT')
        {
            $sql = 'SELECT SQL_NO_CACHE' . substr($sql, 6);
        }

        if (($result = $connection->query($sql)) === false)
        {
            if (isset($benchmark))
            {
                /**
                 * @var $benchmark \MyQEE\Develop\Profiler
                 */
                $benchmark->delete();
            }

            if (INCLUDE_MYQEE_CORE && IS_DEBUG)
            {
                $err = 'Error:' . \mysqli_error($connection) . '. SQL:' . $sql;
            }
            else
            {
                $err = \mysqli_error($connection);
            }

            $this->release();

            throw new Exception($err, \mysqli_errno($connection));
        }

        if (isset($benchmark))
        {
            # 在线查看SQL情况
            if ($isSqlDebug)
            {
                $data = array();
                $data[0]['db']            = $this->connectionId . $this->connections[$this->connectionId]->database;
                $data[0]['select_type']   = '';
                $data[0]['table']         = '';
                $data[0]['key']           = '';
                $data[0]['key_len']       = '';
                $data[0]['Extra']         = '';
                $data[0]['query']         = '';
                $data[0]['sqlType']          = '';
                $data[0]['id']            = '';
                $data[0]['row']           = count($result);
                $data[0]['ref']           = '';
                $data[0]['all rows']      = '';
                $data[0]['possible_keys'] = '';

                if (strtoupper(substr($sql, 0, 6)) === 'SELECT')
                {
                    $re = $connection->query('EXPLAIN ' . $sql);
                    $i = 0;
                    while (true == ($row = $re->fetch_array(MYSQLI_NUM)))
                    {
                        $data[$i]['select_type']      = (string)$row[1];
                        $data[$i]['table']            = (string)$row[2];
                        $data[$i]['key']              = (string)$row[5];
                        $data[$i]['key_len']          = (string)$row[6];
                        $data[$i]['Extra']            = (string)$row[9];
                        if ($i == 0) $data[$i]['query'] = '';
                        $data[$i]['sqlType']          = (string)$row[3];
                        $data[$i]['id']               = (string)$row[0];
                        $data[$i]['ref']              = (string)$row[7];
                        $data[$i]['all rows']         = (string)$row[8];
                        $data[$i]['possible_keys']    = (string)$row[4];
                        $i++;
                    }
                }

                $data[0]['query'] = $sql;
            }
            else
            {
                $data = null;
            }
            if(INCLUDE_MYQEE_CORE && IS_DEBUG)Core::debug()->profiler('sql')->stop($data);
        }

        // Set the last query
        $this->lastQuery = $sql;

        if ($sqlType === 'INSERT' || $sqlType === 'REPLACE')
        {
            // Return a list of insert id and rows created
            return [
                \mysqli_insert_id($connection),
                \mysqli_affected_rows($connection)
            ];
        }
        elseif ($sqlType === 'UPDATE' || $sqlType === 'DELETE')
        {
            // Return the number of rows affected
            return \mysqli_affected_rows($connection);
        }
        else
        {
            // Return an iterator of results
            return new Result($result, $sql, $asObject, $this->config);
        }
    }
}