<?php
namespace MyQEE\Database\MariaDB;

/**
 * 数据库MySQLi返回类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MariaDB
 * @copyright  Copyright (c) 2008-2018 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Result extends \MyQEE\Database\Result
{
    public function free()
    {
        if (is_resource($this->result))
        {
            $this->result->free();
        }
        $this->result = null;
    }


    public function seek($offset)
    {
        if (isset($this->data[$offset]))
        {
            return true;
        }
        elseif ($this->offsetExists($offset) && $this->result && $this->result->data_seek($offset))
        {
            $this->currentRow = $this->internalRow = $offset;

            return true;
        }
        else
        {
            return false;
        }
    }

    public function fetchAssoc()
    {
        return $this->result->fetch_assoc();
    }

    /**
     * 返回一个对象
     *
     * @return object|\stdClass
     */
    public function fetchObject()
    {
        if (is_string($this->asObject))
        {
            return $this->result->fetch_object($this->asObject);
        }
        else
        {
            return $this->result->fetch_object();
        }
    }

    protected function totalCount()
    {
        if ($this->result)
        {
            $count = @$this->result->num_rows;

            if (!$count > 0)$count = 0;
        }
        else
        {
            $count = count($this->data);
        }

        return $count;
    }
}