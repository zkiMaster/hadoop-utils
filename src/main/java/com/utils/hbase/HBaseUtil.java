package com.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {

    private final static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    public static abstract class Option {

        private Configuration conf;
        Connection conn;

        public Option(Configuration conf) {
            try {
                this.conf = conf;
                this.conn = getConnection(conf);
            } catch (Exception e) {
                logger.error("Option 初始化失败 ！！！");
                e.printStackTrace();
            }
        }

        public Connection getConnection(Configuration conf) {
            Connection conn = null;
            try {
                synchronized (Option.class) {
                    conn = ConnectionFactory.createConnection(conf);
                }
            } catch (IOException e) {
                logger.error("获取hbase连接失败！！！");
                e.printStackTrace();
            }
            return conn;
        }

        public void closeConnection(Connection connection) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                connection = null;
            }
        }
    }


    public static class QueryOption extends Option {
        public QueryOption(Configuration conf) {
            super(conf);
        }
    }


    public static class FilterOption extends Option {
        public FilterOption(Configuration conf) {
            super(conf);
        }

        /**
         * 根据列的值返回一个比较器
         *
         * @param compareOp
         * @param family
         * @param qualifier
         * @param value
         * @param missReturnAll 如果为true，当这一列不存在时，不会返回，如果为false
         * @param lastVersion   是否返回之后一个版本
         * @return
         */
        public static Filter getSingleColumnValueFilter(CompareFilter.CompareOp compareOp, String family,
                                                        String qualifier, String value, Boolean missReturnAll, Boolean lastVersion) {
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    compareOp, Bytes.toBytes(value));
            singleColumnValueFilter.setFilterIfMissing(missReturnAll);
            //是否返回最后一个版本
            singleColumnValueFilter.setLatestVersionOnly(lastVersion);
            return singleColumnValueFilter;
        }

        public static Filter getSingleColumnValueExcludeFilter(CompareFilter.CompareOp compareOp, String family,
                                                               String qualifier, String value) {
            SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes(family),
                    Bytes.toBytes(qualifier), compareOp, Bytes.toBytes(value));
            return filter;
        }


        /**
         * 列族过滤器
         *
         * @param compareOp
         * @param family
         * @return
         */
        public static Filter getFamilyFilter(CompareFilter.CompareOp compareOp, String family) {
            FamilyFilter familyFilter =
                    new FamilyFilter(compareOp, new BinaryComparator(Bytes.toBytes(family)));
            return familyFilter;
        }

        /**
         * 列过滤器
         *
         * @param compareOp
         * @param qualifier
         * @return
         */
        public static Filter getQualifierFilter(CompareFilter.CompareOp compareOp, String qualifier) {
            QualifierFilter filter = new QualifierFilter(compareOp, new BinaryComparator(Bytes.toBytes(qualifier)));
            return filter;
        }

        /**
         * 列过滤器
         * 根据 qualifierPrefix 进行模糊匹配
         * 类似于mysql中的 'a%'
         *
         * @param qualifierPrefix
         * @return
         */
        public static Filter getQualifierPrefixFilter(String qualifierPrefix) {
            ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(qualifierPrefix));
            return filter;
        }

        /**
         * 列过滤器
         * 基于多个列名(即Qualifier)前缀过滤数据
         *
         * @param mulQulifierPrefix
         * @return
         */
        public static Filter getMulQualifierPrefixFilter(String... mulQulifierPrefix) {
            MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(Bytes.toByteArrays(mulQulifierPrefix));
            return filter;
        }


        /**
         * 基于列的范围过滤器
         *
         * @param starColumn
         * @param minColumnInclusive 是否包含下边界
         * @param endColumn
         * @param maxColumnInclusive 是否包含下边界
         * @return
         */
        public static Filter getQualifierRangeFilter(String starColumn, boolean minColumnInclusive, String endColumn, boolean maxColumnInclusive) {
            ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(starColumn), minColumnInclusive,
                    Bytes.toBytes(endColumn), maxColumnInclusive);
            return filter;
        }

        /**
         * 行过滤器
         *
         * @param compareOp
         * @param rowKey
         * @return
         */
        public static Filter getRowKeyFilter(CompareFilter.CompareOp compareOp,
                                             String rowKey) {
            RowFilter filter = new RowFilter(compareOp, new SubstringComparator(rowKey));
            return filter;
        }

        /**
         * 返回指定页面的结果集
         *
         * @param pageSize
         * @return
         */
        public static Filter getPageFilter(Long pageSize) {
            PageFilter pageFilter = new PageFilter(pageSize);
            return pageFilter;
        }

        /**
         * 根据整行中的每个列来做过滤，只要存在一列不满足条件，整行都被过滤掉。
         *
         * @param filter
         * @return
         */
        public static Filter getSkipFilter(Filter filter) {
            SkipFilter skipFilter = new SkipFilter(filter);
            return skipFilter;
        }

        /**
         * 当遇到一条数据被过滤时，它就会放弃后面的扫描。
         * 使用封装的过滤器来检查KeyValue,并确认是否一行数据因行键或是列被跳过而过滤。
         * @param filter
         * @return
         */
        public static Filter getWhileMatchFilter(Filter filter){
            WhileMatchFilter matchFilter = new WhileMatchFilter(filter);
            return matchFilter;
        }

        /**
         * 该过滤器仅仅返回每一行中的第一个cell的值，可以用于高效的执行行数统计操作。
         *
         * @return
         */
        public static Filter getFirstOnlyFilter() {
            return new FirstKeyOnlyFilter();
        }


        /**
         * 返回每行的前 num 列
         *
         * @param num
         * @return
         */
        public static Filter getCoulumnPreCountFilter(int num) {
            ColumnCountGetFilter filter = new ColumnCountGetFilter(num);
            return filter;
        }

        /**
         * 返回指定时间序列的列
         *
         * @param timeStamps
         * @return
         */
        public static Filter getTimeStampFilter(List<Long> timeStamps) {
            TimestampsFilter filter = new TimestampsFilter(timeStamps);
            return filter;
        }

        /**
         * 多个list根据指定条件进行串联
         *
         * @param operator
         * @param filters
         * @return
         */
        public static FilterList addFilter(FilterList.Operator operator, Filter... filters) {
            FilterList filterList = new FilterList(operator, Arrays.asList(filters));
            return filterList;
        }

    }

    public static class EditOption extends Option {

        private Admin master;

        public EditOption(Configuration conf) {
            super(conf);
            try {
                master = getConnection(conf).getAdmin();
            } catch (IOException e) {
                logger.error("EditOption 初始化失败！！");
                e.printStackTrace();
            }
        }

        /**
         * 删除rowKey相同的数据
         *
         * @param tableName
         * @param rowKey
         */
        public void deleteRow(String tableName, String rowKey) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                table.delete(delete);
            } catch (IOException e) {
                logger.error("EditOption_$_deleteRow 错误！！！");
                e.printStackTrace();
            }
        }

        /**
         * 删除一行中某一列族中的所有数据
         *
         * @param tableName
         * @param rowKey
         * @param cf
         */
        public void deleteRow(String tableName, String rowKey, String cf) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                delete.addFamily(Bytes.toBytes(cf));
                table.delete(delete);
            } catch (IOException e) {
                logger.error("EditOption_$_deleteRow 错误！！！");
                e.printStackTrace();
            }
        }

        public void delete(String tableName, List<Delete> deletes) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                table.delete(deletes);
            } catch (IOException e) {
                logger.error("");
                e.printStackTrace();
            }
        }

        public void delete(String tableName, Delete delete) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                table.delete(delete);
            } catch (IOException e) {
                logger.error("EditOption_$_");
                e.printStackTrace();
            }
        }

        /**
         * 删除某一列数据
         *
         * @param tableName
         * @param rowKey
         * @param cf
         * @param qualifier
         */
        public void deleteQualifierValue(String tableName, String rowKey, String cf, String qualifier) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
                table.delete(delete);
            } catch (IOException e) {
                logger.error("EditOption_$_deleteQualifierValue 错误！！！");
                e.printStackTrace();
            }

        }

        /**
         * 往多行中的某一列插入相同的数据
         *
         * @param tableName
         * @param cf
         * @param qualifier
         * @param rowList
         * @param value
         */
        public void put(String tableName, String cf, String qualifier
                , List<String> rowList, String value) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                List<Put> puts = new ArrayList<>();
                for (String s : rowList) {
                    Put put = new Put(Bytes.toBytes(s));
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier)
                            , Bytes.toBytes(value));
                    puts.add(put);
                }
                table.put(puts);
            } catch (IOException e) {
                logger.error("EditOption_$_put 错误！！！");
                e.printStackTrace();
            }
        }

        public void put(String tableName, Put put) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                table.put(put);
            } catch (IOException e) {
                logger.error("EditOption_$_put 插入数据失败！！！");
                e.printStackTrace();
            }

        }

        public void put(String tableName, List<Put> puts) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                table.put(puts);
            } catch (IOException e) {
                logger.error("EditOption_$_put 插入数据失败 ！！！");
                e.printStackTrace();
            }

        }

        public void put(String tableName, String rowKey, String columnFamily,
                        String column, String value) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                table.put(put);
            } catch (IOException e) {
                logger.error("EditOption_$_put put错误！！！！");
                e.printStackTrace();
            }
        }

        /**
         * @param tableName
         * @param rowKey
         * @param columnFamily
         * @param list         map的key：列名，value：值
         */
        public void put(String tableName, String rowKey, String columnFamily,
                        List<Map<String, String>> list) {
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));

                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map<String, String> map : list) {
                    for (Map.Entry<String, String> m : map.entrySet()) {
                        put.addColumn(Bytes.toBytes(columnFamily),
                                Bytes.toBytes(m.getKey()), Bytes.toBytes(m.getValue()));
                    }
                }
                table.put(put);
            } catch (IOException e) {
                logger.error("EditOption_$_put 错误！！！");
                e.printStackTrace();
            }

        }
    }

    public static class TableOption extends Option {

        private Admin master;

        public TableOption(Configuration conf) {
            super(conf);
            try {
                this.master = conn.getAdmin();
            } catch (IOException e) {
                logger.error("TableOption创建master失败!!!");
                e.printStackTrace();
            }
        }

        /**
         * 判断表是否存在
         *
         * @param tableName
         * @return
         */
        public boolean isExist(String tableName) {
            try {
                if (master.tableExists(TableName.valueOf(tableName))) {
                    return true;
                }
            } catch (IOException e) {
                logger.error("TableOption_$_isExist error ....");
                e.printStackTrace();
            }
            return false;
        }

        /**
         * 创建表，表的列族不能为空
         *
         * @param tableName
         * @param familyNames
         */
        public void create(String tableName, String... familyNames) {
            if (familyNames.length < 1) {
                logger.error("列族不能为空！！！");
                return;
            }
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : familyNames) {
                HColumnDescriptor family = new HColumnDescriptor(cf);
                desc.addFamily(family);
            }
            try {
                master.createTable(desc);
            } catch (IOException e) {
                logger.error("TableOption_$_create创建表失败！！！");
                e.printStackTrace();
            }
        }


        public void create(HTableDescriptor descriptor) {

            Collection<HColumnDescriptor> families = descriptor.getFamilies();
            if (families.size() < 1) {
                logger.error("列族不能为空！！");
                return;
            }
            try {
                master.createTable(descriptor);
            } catch (IOException e) {
                logger.error("TableOption_$_create 创建表失败！！！");
                e.printStackTrace();
            }
        }

        /**
         * 获取所有的表名
         *
         * @return
         */
        public List<String> listTableName() {
            List<String> list = new ArrayList<>();

            try {
                HTableDescriptor[] tables = master.listTables();
                for (HTableDescriptor table : tables) {
                    String t_name = table.getNameAsString();
                    list.add(t_name);
                }
            } catch (IOException e) {
                logger.error("TableName_$_list获取所有表失败！！！");
                e.printStackTrace();
            }
            return list;
        }

        public HTableDescriptor[] list() {
            HTableDescriptor[] tables = null;
            try {
                tables = master.listTables();
            } catch (IOException e) {
                logger.error("TableOption_$_list 获取list失败！！！");
                e.printStackTrace();
            }
            return tables;
        }

        /**
         * 获取一个表的详细信息
         *
         * @param tableName
         * @return
         */
        public HTableDescriptor describe(String tableName) {
            HTableDescriptor descriptor = null;
            try {
                descriptor = master.getTableDescriptor(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return descriptor;

        }

        public void alter(String tableName, HColumnDescriptor columnDescriptor) {
            HColumnDescriptor column = new HColumnDescriptor(columnDescriptor);
            try {
                master.addColumn(TableName.valueOf(tableName), column);
            } catch (IOException e) {
                logger.error("TableName_$_alter 修改表错误！！！");
                e.printStackTrace();
            }
        }

        public void disable(String tableName) {
            try {
                master.disableTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("TableOption_$_disable 关闭表失败！！！");
                e.printStackTrace();
            }

        }

        public boolean isDisable(String tableName) {
            boolean flag = false;
            try {
                flag = master.isTableDisabled(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("TableOption_$_isDisable 错误！！！");
                e.printStackTrace();
            }
            return flag;
        }

        public void enable(String tableName) {
            try {
                master.enableTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("TableOption_$_enable 错误！！！");
                e.printStackTrace();
            }
        }

        public void isEnable(String tableName) {
            try {
                master.isTableEnabled(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("TableName_$_isEnable 错误！！！");
                e.printStackTrace();
            }
        }

        /**
         * 先disable再删除
         *
         * @param tableName
         * @param isDelete
         * @return
         */
        public boolean drop(String tableName, boolean isDelete) {
            if (isDisable(tableName)) {
                disable(tableName);
            }
            return drop(tableName);
        }

        /**
         * 直接删除，如果表不为disable则返回false
         *
         * @param tableName
         * @return
         */
        public boolean drop(String tableName) {
            boolean disable = isDisable(tableName);
            if (!disable) {
                logger.error("TableOption_$_drop 表" + tableName + " 不是disable ！！！");
                return false;
            }
            try {
                master.deleteTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                logger.error("TableOption_$_drop 错误");
                return false;
            }
            return true;
        }

        public int count(String tableName) {
            int count = 0;
            try {
                Table table = conn.getTable(TableName.valueOf(tableName));
                Scan scan = new Scan();
                //只获取每一行的第一个cell
                scan.setFilter(FilterOption.getFirstOnlyFilter());
                ResultScanner resultScanner = table.getScanner(scan);
                for (Result result : resultScanner) {
                    //统计
                    List<Cell> cells = result.listCells();
                    count += cells.size();
                }
            } catch (IOException e) {
                logger.error("TableOption_$_count 错误！！！");
                e.printStackTrace();
            }
            return count;
        }
    }


    /**
     * namespace操作类
     */
    public static class NamespaceOption extends Option {

        private Admin master;

        public NamespaceOption(Configuration conf) {
            super(conf);
            try {
                this.master = super.conn.getAdmin();
            } catch (IOException e) {
                logger.error("NamespaceOption 错误！！！");
                e.printStackTrace();
            }
        }

        /**
         * 创建namespace
         *
         * @param namespace
         */
        public void createNamespace(String namespace) {
            if (isExist(namespace)) {
                logger.error(namespace + " namespace已经存在！！");
                return;
            }
            NamespaceDescriptor ns = NamespaceDescriptor.create(namespace).build();
            try {
                master.createNamespace(ns);
            } catch (IOException e) {
                logger.error("Namespace_$_createNamespace 错误 !!!");
                e.printStackTrace();
            }
        }

        public boolean isExist(String namespace) {
            NamespaceDescriptor descriptor = null;
            try {
                descriptor = master.getNamespaceDescriptor(namespace);
            } catch (IOException e) {
                return false;
            }
            return descriptor == null ? false : true;
        }

        /**
         * 获取所有的namespace
         *
         * @return
         */
        public List<String> listNamespaces() {
            List<String> list = new ArrayList<>();
            try {
                NamespaceDescriptor[] namespaces = master.listNamespaceDescriptors();
                for (NamespaceDescriptor namespace : namespaces) {
                    list.add(namespace.getName());
                }
            } catch (IOException e) {
                logger.error("Namespace_$_listNamespaces 错误！！！");
                e.printStackTrace();
            }
            return list;
        }

        /**
         * 获取一个namespace中的所有表
         *
         * @param namespace
         * @return
         */
        public TableName[] listNamespaceTables(String namespace) {
            TableName[] tableNames = null;
            try {
                tableNames = master.listTableNamesByNamespace(namespace);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return tableNames;
        }

        /**
         * 删除namespace
         *
         * @param namespace
         * @return
         */
        public boolean dropNamespace(String namespace) {
            try {
                master.deleteNamespace(namespace);
            } catch (IOException e) {
                logger.error("Namespace_$_dropNamespace 错误,namespace中的表数据不为空，无法删除！！！");
                return false;
            }
            return true;
        }
    }

}
