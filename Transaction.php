<?php

namespace MyQEE\Database\MySQLi;

use \Exception;

/**
 * MySQLi 事务
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MySQLi
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Transaction extends \MyQEE\Database\Transaction
{
    /**
     * 当前连接ID
     * @var string
     */
    protected $connectionId;

    /**
     * 记录事务
     *
     *      [
     *          '连接ID'=>'父事务ID',
     *          '连接ID'=>'父事务ID',
     *          ...
     *      ]
     *
     * @var array
     */
    protected static $transactions = [];

    /**
     * 开启事务
     *
     * @return $this
     */
    public function start()
    {
        if ($this->id)
        {
            throw new Exception('transaction has started');
        }

        # 获取连接ID
        $this->connectionId = $this->driver->connection(true);

        # 获取唯一ID
        $this->id = uniqid('TaId_' . rand());

        if (isset(static::$transactions[$this->connectionId]))
        {
            # 已存在事务，则该事务为子事务
            if ($this->setSavePoint())
            {
                //保存事务点
                static::$transactions[$this->connectionId][$this->id] = true;
            }
            else
            {
                $this->id = null;
                # 开启事务失败。
                throw new Exception('start sub transaction error');
            }
        }
        else
        {
            # 开启新事务
            $this->query('SET AUTOCOMMIT=0;');

            if (true === $this->query('START TRANSACTION;'))
            {
                # 如果没有建立到当前主服务器的连接，该操作会隐式的建立
                static::$transactions[$this->connectionId] = [$this->id => true];
            }
            else
            {
                $this->id = null;

                # 开启事务失败
                throw new Exception('start transaction error');
            }
        }

        return true;
    }

    /**
     * 提交事务，支持子事务
     *
     * @return Boolean true:成功
     * @throws Exception
     */
    public function commit()
    {
        if (!$this->id || ! $this->haveId()) return false;

        if ($this->isRoot())
        {
            # 父事务
            while (count(static::$transactions[$this->connectionId]) > 1)
            {
                # 还有没有提交的子事务
                end(static::$transactions[$this->connectionId]);

                $subId = key(static::$transactions[$this->connectionId]);

                if (!$this->releaseSavePoint($subId))
                {
                    throw new Exception('commit error');
                }
            }
            $status = $this->query('COMMIT;');
            $this->query('SET AUTOCOMMIT=1;');

            if ($status)
            {
                unset(static::$transactions[$this->connectionId]);
            }
        }
        else
        {
            # 子事务
            $status = $this->releaseSavePoint($this->id);
        }
        if ($status)
        {
            $this->id = null;
            return true;
        }
        else
        {
            throw new Exception('not commit transaction');
        }
    }

    /**
     * 撤消事务，支持子事务
     *
     * @return bool true:成功；false:失败
     */
    public function rollback()
    {
        if (!$this->id) return false;
        if (!$this->haveId()) return false;

        if ($this->isRoot())
        {
            # 父事务
            $status = $this->query('ROLLBACK;');
            $this->query('SET AUTOCOMMIT=1;');
            if ($status)
            {
                unset(static::$transactions[$this->connectionId]);
            }
        }
        else
        {
            # 子事务
            $status = $this->query("ROLLBACK TO SAVEPOINT {$this->id};");

            $this->releaseSavePoint($this->id);
        }
        if ($status)
        {
            $this->id = null;
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * 是否父事务
     */
    public function isRoot()
    {
        if (!$this->id) return false;

        return isset(static::$transactions[$this->connectionId]) && key(static::$transactions[$this->connectionId]) == $this->id;
    }

    /**
     * 设置子事务的保存点，用于支持子事务的回滚
     *
     * @return Boolean  true:成功；false:失败
     */
    protected function setSavePoint()
    {
        if (!$this->isRoot())
        {
            //只有子事务才需要保存点
            if (true === $this->query("SAVEPOINT {$this->id};"))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * 释放事务保存点
     * @return Boolean  true:成功；false:失败
     */
    protected function releaseSavePoint($id)
    {
        if (!$this->isRoot())
        {
            if (true === $this->query("RELEASE SAVEPOINT {$id};"))
            {
                unset(static::$transactions[$this->connectionId][$id]);
                return true;
            }
        }
        return false;
    }

    /**
     * 在事务列表中是否存在
     * @return boolean
     */
    protected function haveId()
    {
        return isset(static::$transactions[$this->connectionId]) && isset(static::$transactions[$this->connectionId][$this->id]);
    }

}