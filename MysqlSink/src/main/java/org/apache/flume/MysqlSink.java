package org.apache.flume;

import com.google.common.base.Preconditions;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.http.HTTPMetricsServer;
import org.apache.flume.sink.AbstractSink;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MysqlSink extends AbstractSink implements Configurable {
    private Connection connect;
    private Statement pstmt;
    private String columnName;
    private String url;
    private String user;
    private String password;
    private String tableName;
    private Map columnMap;
    private HTTPMetricsServer httpMetricsServer;
    private String insertSql;
    private int mysqlBatchSize;
    private int addBatchTime = 0;

    // 在整个sink结束时执行一遍
    @Override
    public synchronized void stop() {
        // TODO Auto-generated method stub
        super.stop();
    }

    // 在整个sink开始时执行一遍
    @Override
    public synchronized void start() {
        // TODO Auto-generated method stub
        super.start();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection(url, user, password);
            // 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
            pstmt = connect.prepareStatement("");
            connect.setAutoCommit(false);
            columnMap = new HashMap();
            //httpMetricsServer =new HTTPMetricsServer();
            //httpMetricsServer.start();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    // 不断循环调用
    public Status process() throws EventDeliveryException {
        // TODO Auto-generated method stub

        Channel ch = getChannel();
        //ch-sink的事务
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            String body = new String(event.getBody());

            if (!columnMap.containsKey(tableName)){
                System.out.println("tableName:"+tableName);
                System.out.println(body);
                String sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name ="+"'"+tableName.toString().split("\\.")[1]+"'";
                ResultSet resultSet = pstmt.executeQuery(sql);
                //columnMap.put(tableName,resultSet.next());
                String columns="";
                while(resultSet.next()){
                    columns+=resultSet.getString(1)+",";
                }

                columns = columns.substring(0,columns.lastIndexOf(","));
                columnMap.put(tableName,columns);
                //System.out.println("columnName:"+columns);
            }

            String setColumns="";

            for (String str : body.split("\\,")) {
                if(columnMap.get(tableName).toString().contains(str.replace("`","").split("\\=")[0]))
                    setColumns += str+",";
            }
            //System.out.println("setColumns:"+setColumns);

            setColumns = setColumns.substring(0,setColumns.lastIndexOf(","));


            if (/*body.split(",").length == columnName.split(",").length*/true) {
                insertSql = "insert ignore into " + tableName + " set " + setColumns;
               // System.out.println("================================================");
               // System.out.println(sql);
                try{
                    pstmt.addBatch(insertSql);
                    addBatchTime ++;
                    if (addBatchTime == mysqlBatchSize){
                        addBatchTime = 0;
                        pstmt.executeBatch();
                        connect.commit();
                    }
                }catch (MySQLSyntaxErrorException e){
                    if(!e.getMessage().contains(" Unknown column"))
                        throw e;
                }
                txn.commit();
                return Status.READY;
            } else {
                txn.rollback();
                return null;
            }
        } catch (Throwable th) {
             System.out.println("================================================");
             System.out.println(insertSql);
            System.out.println("================================================");
            txn.rollback();

            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
    }

    public void configure(Context arg0) {
       // columnName = arg0.getString("column_name");
       // Preconditions.checkNotNull(columnName, "column_name must be set!!");
        url = arg0.getString("url");
        Preconditions.checkNotNull(url, "url must be set!!");
        user = arg0.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = arg0.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        tableName = arg0.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        mysqlBatchSize=arg0.getInteger("mysqlBatchSize");
        Preconditions.checkNotNull(mysqlBatchSize, "mysqlBatchSize must be set!!");
    }

}