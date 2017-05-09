<?php
// $Id: mysql.php 2403 2009-04-07 03:52:48Z dualface $

/**
 * 定义 QDB_Adapter_Mysql 类
 *
 * @link http://qeephp.com/
 * @copyright Copyright (c) 2006-2009 Qeeyuan Inc. {@link http://www.qeeyuan.com}
 * @license New BSD License {@link http://qeephp.com/license/}
 * @version $Id: mysql.php 2403 2009-04-07 03:52:48Z dualface $
 * @package database
 */

/**
 * QDB_Mysql 提供了对 mysql 数据库的支持
 *
 * @author YuLei Liao <liaoyulei@qeeyuan.com>
 * @version $Id: mysql.php 2403 2009-04-07 03:52:48Z dualface $
 * @package database
 */
class QDB_Adapter_Pdo_Mysql extends QDB_Adapter_Pdo_Abstract {
    protected $_pdo_type = 'mysql';
    protected $_bind_enabled = false;

    function __construct($dsn, $id)
    {
        if (! is_array($dsn))
        {
            $dsn = QDB::parseDSN($dsn);
        }
        parent::__construct($dsn, $id);
        $this->_schema = $dsn['database'];
    }

    function identifier($name)
    {
        return ($name != '*') ? "`{$name}`" : '*';
    }

    function nextID($table_name, $field_name, $start_value = 1)
    {
        $seq_table_name = $this->qid("{$table_name}_{$field_name}_seq");
        $next_sql = sprintf('UPDATE %s SET id = LAST_INSERT_ID(id + 1)', $seq_table_name);
        $start_value = intval($start_value);

        $successed = false;
        try
        {
            // 首先产生下一个序列值
            $this->execute($next_sql);
            if ($this->affectedRows() > 0)
            {
                $successed = true;
            }
        }
        catch (QDB_Exception $ex)
        {
            // 产生序列值失败，创建序列表
            $this->execute(sprintf('CREATE TABLE %s (id INT NOT NULL)', $seq_table_name));
        }

        if (! $successed)
        {
            // 没有更新任何记录或者新创建序列表，都需要插入初始的记录
            if ($this->getOne(sprintf('SELECT COUNT(*) FROM %s', $seq_table_name)) == 0)
            {
                $sql = sprintf('INSERT INTO %s VALUES (%s)', $seq_table_name, $start_value);
                $this->execute($sql);
            }
            $this->execute($next_sql);
        }
        // 获得新的序列值
        $this->_insert_id = $this->insertID();
        return $this->_insert_id;
    }

    function createSeq($seq_name, $start_value = 1)
    {
        $seq_table_name = $this->qid($seq_name);
        $this->execute(sprintf('CREATE TABLE %s (id INT NOT NULL)', $seq_table_name));
        $this->execute(sprintf('INSERT INTO %s VALUES (%s)', $seq_table_name, $start_value));
    }

    function dropSeq($seq_name)
    {
        $this->execute(sprintf('DROP TABLE %s', $this->qid($seq_name)));
    }

    function insertID()
    {
        //需要修改
        return $this->_conn->lastInsertId();
    }


    function selectLimit($sql, $offset = 0, $length = 30, array $inputarr = null)
    {
        if (! is_null($offset))
        {
            $sql .= ' LIMIT ' . (int) $offset;
            if (! is_null($length))
            {
                $sql .= ', ' . (int) $length;
            }
            else
            {
                $sql .= ', 18446744073709551615';
            }
        }
        elseif (! is_null($length))
        {
            $sql .= ' LIMIT ' . (int) $length;
        }
        return $this->execute($sql, $inputarr);
    }

    function startTrans()
    {
        if (! $this->_transaction_enabled)
        {
            return false;
        }
        if ($this->_trans_count == 0)
        {
            $this->execute('START TRANSACTION');
            $this->_has_failed_query = false;
        }
        elseif ($this->_trans_count && $this->_savepoint_enabled)
        {
            $savepoint = 'savepoint_' . $this->_trans_count;
            $this->execute("SAVEPOINT `{$savepoint}`");
            array_push($this->_savepoints_stack, $savepoint);
        }
        ++ $this->_trans_count;
        return true;
    }

    function completeTrans($commit_on_no_errors = true)
    {
        if ($this->_trans_count == 0)
        {
            return;
        }
        -- $this->_trans_count;
        if ($this->_trans_count == 0)
        {
            if ($this->_has_failed_query == false && $commit_on_no_errors)
            {
                $this->execute('COMMIT');
            }
            else
            {
                $this->execute('ROLLBACK');
            }
        }
        elseif ($this->_savepoint_enabled)
        {
            $savepoint = array_pop($this->_savepoints_stack);
            if ($this->_has_failed_query || $commit_on_no_errors == false)
            {
                $this->execute("ROLLBACK TO SAVEPOINT `{$savepoint}`");
            }
        }
    }

    function metaColumns($table_name)
    {
        static $type_mapping = array(
            'bit'           => 'int1',
            'tinyint'       => 'int1',
            'bool'          => 'bool',
            'boolean'       => 'bool',
            'smallint'      => 'int2',
            'mediumint'     => 'int3',
            'int'           => 'int4',
            'integer'       => 'int4',
            'bigint'        => 'int8',
            'float'         => 'float',
            'double'        => 'double',
            'doubleprecision' => 'double',
            'float unsigned' => 'float',
            'decimal'       => 'dec',
            'dec'           => 'dec',

            'date'          => 'date',
            'datetime'      => 'datetime',
            'timestamp'     => 'timestamp',
            'time'          => 'time',
            'year'          => 'int2',

            'char'          => 'char',
            'nchar'         => 'char',
            'varchar'       => 'varchar',
            'nvarchar'      => 'varchar',
            'binary'        => 'binary',
            'varbinary'     => 'varbinary',
            'tinyblob'      => 'blob',
            'tinytext'      => 'text',
            'blob'          => 'blob',
            'text'          => 'text',
            'mediumblob'    => 'blob',
            'mediumtext'    => 'text',
            'longblob'      => 'blob',
            'longtext'      => 'text',
            'enum'          => 'enum',
            'set'           => 'set'
        );

        $rs = $this->execute(sprintf('SHOW FULL COLUMNS FROM %s', $this->qid($table_name)));


        $retarr = array();
        $rs->fetch_mode = QDB::FETCH_MODE_ASSOC;
        $rs->result_field_name_lower = true;
        
        while (($row = $rs->fetchRow()))
        {
            $field = array();
            $field['name'] = $row['Field'];
            $type = strtolower($row['Type']);

            $field['scale'] = null;
            $query_arr = false;
            if (preg_match('/^(.+)\((\d+),(\d+)/', $type, $query_arr))
            {
                $field['type'] = $query_arr[1];
                $field['length'] = is_numeric($query_arr[2]) ? $query_arr[2] : - 1;
                $field['scale'] = is_numeric($query_arr[3]) ? $query_arr[3] : - 1;
            }
            elseif (preg_match('/^(.+)\((\d+)/', $type, $query_arr))
            {
                $field['type'] = $query_arr[1];
                $field['length'] = is_numeric($query_arr[2]) ? $query_arr[2] : - 1;
            }
            elseif (preg_match('/^(enum)\((.*)\)$/i', $type, $query_arr))
            {
                $field['type'] = $query_arr[1];
                $arr = explode(",", $query_arr[2]);
                $field['enums'] = $arr;
                $zlen = max(array_map("strlen", $arr)) - 2; // PHP >= 4.0.6
                $field['length'] = ($zlen > 0) ? $zlen : 1;
            }
            else
            {
                $field['type'] = $type;
                $field['length'] = - 1;
            }

            $field['ptype'] = $type_mapping[strtolower($field['type'])];
            $field['not_null'] = (strtolower($row['Null']) != 'yes');
            $field['pk'] = (strtolower($row['Key']) == 'pri');
            $field['auto_incr'] = (strpos($row['Extra'], 'auto_incr') !== false);
            if ($field['auto_incr'])
            {
                $field['ptype'] = 'autoincr';
            }
            $field['binary'] = (strpos($type, 'blob') !== false);
            $field['unsigned'] = (strpos($type, 'unsigned') !== false);

            $field['has_default'] = $field['default'] = null;
            if (! $field['binary'])
            {
                $d = $row['Default'];
                if (!is_null($d) && strtolower($d) != 'null')
                {
                    $field['has_default'] = true;
                    $field['default'] = $d;
                }
                
                //--start--- by jackwu at 2012/04/20 email:262348314@qq.com
                //当字段null属性为not null，并且没有设置默认值时，将其默认值改成''而不是null
                //这样插入空值将不会报错
                if($field['not_null'] && is_null($row['Default']))
                {
                	$field['default'] = '';
                }
                //--start---
            }

            if ($field['type'] == 'tinyint' && $field['length'] == 1)
            {
                $field['ptype'] = 'bool';
            }

            $field['desc'] = ! empty($row['Comment']) ? $row['Comment'] : '';
            if (! is_null($field['default']))
            {
                switch ($field['ptype'])
                {
                case 'int1':
                case 'int2':
                case 'int3':
                case 'int4':
                    $field['default'] = intval($field['default']);
                    break;
                case 'float':
                case 'double':
                case 'dec':
                    $field['default'] = doubleval($field['default']);
                    break;
                case 'bool':
                    $field['default'] = (bool) $field['default'];
                }
            }

            $retarr[strtolower($field['name'])] = $field;
        }

        return $retarr;
    }

    function metaTables($pattern = null, $schema = null)
    {
        $sql = 'SHOW TABLES';
        if ($schema != '')
        {
            $sql .= " FROM `{$schema}`";
        }
        if ($pattern != '')
        {
            $sql .= ' LIKE ' . $this->qstr($pattern);
        }
        return $this->getCol($sql);
    }

    protected function _fakebind($sql, $inputarr)
    {
        $arr = explode('?', $sql);
        $sql = array_shift($arr);
        foreach ($inputarr as $value)
        {
            if (isset($arr[0]))
            {
                $sql .= $this->qstr($value) . array_shift($arr);
            }
        }
        return $sql;
    }
}


