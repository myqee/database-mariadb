<?php
namespace MyQEE\Database\MySQLI;

use MyQEE\Database\Driver;
use \Exception;

/**
 * 数据库MySQLI返回类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MySQLI
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Factory extends Driver
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
    protected $mysql = true;

    /**
     * 记录当前连接所对应的数据库
     * @var array
     */
    protected static $currentDatabases = array();

    /**
     * 记录当前数据库所对应的页面编码
     * @var array
     */
    protected static $currentCharset = array();

    /**
     * 链接寄存器
     * @var array
     */
    protected static $connectionInstance = array();

    /**
     * 链接寄存器使用数
     *
     * @var array
     */
    protected static $_connectionInstanceCount = array();

    /**
     * 记录connection id所对应的hostname
     *
     * @var array
     */
    protected static $_currentConnectionIdToHostname = array();

    /**
     * 连接数据库
     *
     * $use_connection_type 默认不传为自动判断，可传true/false,若传字符串(只支持a-z0-9的字符串)，则可以切换到另外一个连接，比如传other,则可以连接到$this->_connection_other_id所对应的ID的连接
     *
     * @param boolean $useConnectionType 是否使用主数据库
     */
    public function connect($useConnectionType = null)
    {
        if (null !== $useConnectionType)
        {
            $this->_setConnectionType($useConnectionType);
        }

        $connectionId = $this->connectionId();

        # 最后检查连接时间
        static $lastCheckConnectTime = 0;

        if (!$connectionId || !isset(static::$connectionInstance[$connectionId]))
        {
            $this->_connect();
        }

        # 如果有当前连接，检查连接
        if ($lastCheckConnectTime > 0 && time() - $lastCheckConnectTime >= 5)
        {
            # 5秒后检查一次连接状态
            $this->_checkConnect();
        }

        # 设置编码
        $this->setCharset($this->config['charset']);

        # 切换表
        $this->selectDatabase($this->config['connection']['database']);

        $lastCheckConnectTime = time();
    }

    /**
     * 获取当前连接
     *
     * @return \mysqli
     */
    public function connection()
    {
        # 尝试连接数据库
        $this->connect();

        # 获取连接ID
        $connectionId = $this->connectionId();

        if ($connectionId && isset(static::$connectionInstance[$connectionId]))
        {
            return static::$connectionInstance[$connectionId];
        }
        else
        {
            throw new Exception('数据库连接异常');
        }
    }

    protected function _connect()
    {
        if ($this->_tryUseExistsConnection())
        {
            return;
        }

        $database = $hostname = $port = $socket = $username = $password = $persistent = null;
        extract($this->config['connection']);

        # 错误服务器
        static $errorHost = array();

        $last_error = null;
        while (true)
        {
            $hostname = $this->_getRandHost($errorHost);

            if (false === $hostname)
            {
                if(HAVE_MYQEE_CORE && IS_DEBUG)Core::debug()->warn($errorHost, 'errorHost');

                if ($last_error && $last_error instanceof Exception)throw $last_error;
                throw new Exception('connect mysqli server error.');
            }

            $connectionId = $this->_getConnectionHash($hostname, $port, $username);
            static::$_currentConnectionIdToHostname[$connectionId] = $username .'@'. $hostname .':'. $port;

            try
            {
                $time = microtime(true);

                $error_code = 0;
                $error_msg  = '';
                try
                {
                    if (empty($persistent))
                    {
                        $tmpLink = mysqli_init();
                        mysqli_options($tmpLink, MYSQLI_OPT_CONNECT_TIMEOUT, 3);
                        mysqli_real_connect($tmpLink, $hostname, $username, $password, $database, $port, null, \MYSQLI_CLIENT_COMPRESS);
                    }
                    else
                    {
                        $tmpLink = new \mysqli($hostname, $username, $password, $database, $port);
                    }
                }
                catch (Exception $e)
                {
                    $error_msg  = $e->getMessage();
                    $error_code = $e->getCode();
                    $tmpLink    = false;
                }

                if (false === $tmpLink)
                {
                    if (HAVE_MYQEE_CORE && IS_DEBUG)throw $e;

                    if (!($error_msg && 2===$error_code && preg_match('#(Unknown database|Access denied for user)#i', $error_msg)))
                    {
                        $error_msg = 'connect mysqli server error.';
                    }
                    throw new Exception($error_msg, $error_code);
                }

                if (HAVE_MYQEE_CORE && IS_DEBUG)Core::debug()->info("mysqli://{$username}@{$hostname}:{$port}/{$database}/ connection time: " . (microtime(true) - $time));

                # 连接ID
                $this->connectionIds[$this->connectionType] = $connectionId;

                # 设置实例化对象
                static::$connectionInstance[$connectionId] = $tmpLink;

                # 设置当前连接的数据库
                static::$currentDatabases[$connectionId] = $database;

                # 设置计数器
                static::$_connectionInstanceCount[$connectionId] = 1;

                unset($tmpLink);

                break;
            }
            catch (Exception $e)
            {
                if (HAVE_MYQEE_CORE && IS_DEBUG)
                {
                    Core::debug()->error($username.'@'.$hostname.':'.$port.'.Msg:'.strip_tags($e->getMessage(), '') .'.Code:'. $e->getCode(), 'connect mysqli server error');
                    $last_error = new Exception($e->getMessage(), $e->getCode());
                }
                else
                {
                    $last_error = new Exception('connect mysqli server error', $e->getCode());
                }

                if (2 === $e->getCode() && preg_match('#(Unknown database|Access denied for user)#i', $e->getMessage(), $m))
                {
                    // 指定的库不存在，直接返回
                    throw new Exception(strtolower($m[1]) === 'unknown database' ? __('The mysql database does not exist') : __('The mysql database account or password error'));
                }
                else
                {
                    if (!in_array($hostname, $errorHost))
                    {
                        $errorHost[] = $hostname;
                    }
                }
            }
        }
    }

    /**
     * @return bool
     * @throws Exception
     */
    protected function _tryUseExistsConnection()
    {
        # 检查下是否已经有连接连上去了
        if (static::$connectionInstance)
        {
            $hostname = $this->config['connection']['hostname'];
            if (is_array($hostname))
            {
                $hostConfig = $hostname[$this->connectionType];
                if (!$hostConfig)
                {
                    throw new Exception('指定的数据库连接主从配置中('.$this->connectionType.')不存在，请检查配置');
                }

                if (!is_array($hostConfig))
                {
                    $hostConfig = [$hostConfig];
                }
            }
            else
            {
                $hostConfig = [$hostname];
            }

            # 先检查是否已经有相同的连接连上了数据库
            foreach ($hostConfig as $host)
            {
                $connectionId = $this->_getConnectionHash($host, $this->config['connection']['port'], $this->config['connection']['username']);

                if (isset(static::$connectionInstance[$connectionId]))
                {
                    $this->connectionIds[$this->connectionType] = $connectionId;

                    # 计数器+1
                    static::$_connectionInstanceCount[$connectionId]++;

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 检查连接是否可用
     *
     * 防止因长时间不链接而导致连接丢失的问题 MySQL server has gone away
     *
     * @throws Exception
     */
    protected function _checkConnect()
    {
        # 5秒检测1次
        static $error_num = 0;
        try
        {
            $connectionId = $this->connectionId();
            $connection   = static::$connectionInstance[$connectionId];

            if ($connection)
            {
                $pingStatus = \mysqli_ping($connection);
            }
            else
            {
                $pingStatus = false;
            }
        }
        catch (Exception $e)
        {
            $error_num++;
            $pingStatus = false;
        }

        if (!$pingStatus)
        {
            if ($error_num < 5)
            {
                $this->closeConnect();

                # 等待3毫秒
                usleep(3000);

                # 再次尝试连接
                $this->connect();
                $error_num = 0;
            }
            else
            {
                throw new Exception('connect mysqli server error');
            }
        }

    }

    /**
     * 关闭链接
     */
    public function closeConnect()
    {
        if ($this->connectionIds)foreach ($this->connectionIds as $key=> $connectionId)
        {
            if ($connectionId && static::$connectionInstance[$connectionId])
            {
                if (isset(static::$_connectionInstanceCount[$connectionId]) && static::$_connectionInstanceCount[$connectionId]>1)
                {
                    static::$_connectionInstanceCount[$connectionId]--;
                }
                else
                {
                    $link = static::$connectionInstance[$connectionId];
                    $id   = static::$_currentConnectionIdToHostname[$connectionId];

                    unset(static::$connectionInstance[$connectionId]);
                    unset(static::$_connectionInstanceCount[$connectionId]);
                    unset(static::$currentDatabases[$connectionId]);
                    unset(static::$currentCharset[$connectionId]);
                    unset(static::$_currentConnectionIdToHostname[$connectionId]);

                    try
                    {
                        \mysqli_close($link);
                    }
                    catch(Exception $e){}

                    unset($link);

                    if(HAVE_MYQEE_CORE && IS_DEBUG)Core::debug()->info('close '. $key .' mysqli '. $id .' connection.');
                }
            }

            $this->connectionIds[$key] = null;
        }
    }

    /**
     * 切换表
     *
     * @param string \MyQEE\Database\DB
     * @return void
     */
    public function selectDatabase($database)
    {
        if (!$database)return;

        $connection_id = $this->connectionId();

        if (!$connection_id || !isset(static::$currentDatabases[$connection_id]) || $database !== static::$currentDatabases[$connection_id])
        {
            $connection = static::$connectionInstance[$connection_id];

            if (!$connection)
            {
                $this->connect();
                $this->selectDatabase($database);
                return;
            }

            if (!\mysqli_select_db($connection, $database))
            {
                throw new Exception('选择数据表错误:' . \mysqli_error($connection), \mysqli_errno($connection));
            }

            if (HAVE_MYQEE_CORE && IS_DEBUG)
            {
                $host = $this->getHostnameByConnectionHash($this->connectionId());
                Core::debug()->info(($host['username']?$host['username'].'@':'') . $host['hostname'] . ($host['port'] && $host['port']!='3306'?':'.$host['port']:'').'select to db:'.$database);
            }

            # 记录当前已选中的数据库
            static::$currentDatabases[$connection_id] = $database;
        }
    }


    /**
     * 设置编码
     *
     * @param string $charset
     * @throws Exception
     * @return void|boolean
     */
    public function setCharset($charset)
    {
        if (!$charset)return false;

        $connectionId = $this->connectionId();
        $connection   = static::$connectionInstance[$connectionId];

        if (!$connectionId || !$connection)
        {
            $this->connect();
            return $this->setCharset($charset);
        }

        if (isset(static::$currentCharset[$connectionId]) && $charset == static::$currentCharset[$connectionId])
        {
            return true;
        }

        $status = mysqli_set_charset($connection, $charset);
        if (false === $status)
        {
            throw new Exception('Error:' . \mysqli_error($connection), \mysqli_errno($connection));
        }

        # 记录当前设置的编码
        static::$currentCharset[$connectionId] = $charset;

        return true;
    }

    public function escape($value)
    {
        $connection = $this->connection();

        $this->changeCharset($value);

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
     * @param string $sql 查询语句
     * @param string $asObject 是否返回对象
     * @param boolean $connectionType 是否使用主数据库，不设置则自动判断
     * @return \MyQEE\Database\Driver\MySQLI\Result
     */
    public function query($sql, $asObject = null, $connectionType = null)
    {
        $sql  = trim($sql);
        $type = $this->_getQueryType($sql, $connectionType);

        # 设置连接类型
        $this->_setConnectionType($connectionType);

        # 连接数据库
        $connection = $this->connection();

        # 记录调试
        if(HAVE_MYQEE_CORE && IS_DEBUG)
        {
            Core::debug()->info($sql, 'MySQL');

            static $isSqlDebug = null;

            if (null === $isSqlDebug) $isSqlDebug = (bool)Core::debug()->profiler('sql')->is_open();

            if ($isSqlDebug)
            {
                $host      = $this->getHostnameByConnectionHash($this->connectionId());
                $benchmark = Core::debug()->profiler('sql')->start('Database', 'mysqli://' . ($host['username']?$host['username'].'@':'') . $host['hostname'] . ($host['port'] && $host['port'] != '3306' ? ':' . $host['port'] : ''));
            }
        }

        static $isNoCache = null;

        if (null === $isNoCache) $isNoCache = (bool)Core::debug()->profiler('nocached')->is_open();

        //显示无缓存数据
        if ($isNoCache && strtoupper(substr($sql, 0, 6)) === 'SELECT')
        {
            $sql = 'SELECT SQL_NO_CACHE' . substr($sql, 6);
        }

        // Execute the query
        if (($result = \mysqli_query($connection, $sql)) === false)
        {
            if (isset($benchmark))
            {
                // This benchmark is worthless
                $benchmark->delete();
            }

            if (HAVE_MYQEE_CORE && IS_DEBUG)
            {
                $err = 'Error:' . \mysqli_error($connection) . '. SQL:' . $sql;
            }
            else
            {
                $err = \mysqli_error($connection);
            }
            throw new Exception($err, \mysqli_errno($connection));
        }

        if (isset($benchmark))
        {
            # 在线查看SQL情况
            if ($isSqlDebug)
            {
                $data = array();
                $data[0]['db']            = $host['hostname'] . '/' . $this->config['connection']['database'] . '/';
                $data[0]['select_type']   = '';
                $data[0]['table']         = '';
                $data[0]['key']           = '';
                $data[0]['key_len']       = '';
                $data[0]['Extra']         = '';
                $data[0]['query']         = '';
                $data[0]['type']          = '';
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
                        if ($i==0) $data[$i]['query'] = '';
                        $data[$i]['type']             = (string)$row[3];
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
            if(HAVE_MYQEE_CORE && IS_DEBUG)Core::debug()->profiler('sql')->stop($data);
        }

        // Set the last query
        $this->lastQuery = $sql;

        if ($type === 'INSERT' || $type === 'REPLACE')
        {
            // Return a list of insert id and rows created
            return [
                \mysqli_insert_id($connection),
                \mysqli_affected_rows($connection)
            ];
        }
        elseif ($type === 'UPDATE' || $type === 'DELETE')
        {
            // Return the number of rows affected
            return \mysqli_affected_rows($connection);
        }
        else
        {
            // Return an iterator of results
            return new Driver_MySQLI_Result($result, $sql, $asObject, $this->config);
        }
    }
}