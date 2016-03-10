<?php
namespace MyQEE\Database\MySQLi;

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
class Result extends \MyQEE\Database\Result
{
    protected function releaseResource()
    {
        if (is_resource($this->result))
        {
            \mysqli_free_result($this->result);
        }
        $this->result = null;
    }

    protected function totalCount()
    {
        if ($this->result)
        {
            $count = @mysqli_num_rows($this->result);
            if (!$count>0)$count = 0;
        }
        else
        {
            $count = count($this->data);
        }

        return $count;
    }

    public function seek($offset)
    {
        if (isset($this->data[$offset]))
        {
            return true;
        }
        elseif ($this->offsetExists($offset) && $this->result && \mysqli_data_seek($this->result, $offset))
        {
            $this->currentRow = $this->internalRow = $offset;

            return true;
        }
        else
        {
            return false;
        }
    }

    protected function fetchAssoc()
    {
        return \mysqli_fetch_assoc($this->result);
    }
}