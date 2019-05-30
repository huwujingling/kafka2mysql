package com.csot.flume.intercepter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.zookeeper.CreateMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class Kafka2MysqlIntercepter implements Interceptor {
    static Map<String, String> confMap;//配置文件中拦截器配置映射
    private Map<String, String> eventMap;//传输信息到zk
    Gson gson = new Gson();
    private String confName;//flume配置文件中拦截器的配置参数.提供配置文件的名字
    private String parentNode;//zk父节点
    private String zkHost;//zk足迹
    private String receiver;//收件人
    private String monitorPort;//监控端口
    private String flumeWatchCycle;
    private ZkClient zkClient;
    private String zkData;
    private Map bodyMap;
    private StringBuilder eventBody ;
    private JsonParser jsonParser;
    private List<Event> results;
    private JsonObject returnData;
    private  String[] arr;
    private  String[] split;
    private StringBuilder keyValues ;
    private String eqp;
    private int count = 1;


    public void initialize() {
        try {
            //獲取並檢查zkHost
            zkHost = confMap.get("zkHost");
            if (zkHost == null || zkHost == "")
                throw new Exception("\n=====================================================================================\n"
                        + "--zkHost is empty ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            try {
                //注册zk服务
                zkClient = new ZkClient(zkHost, 6000, 15000);
            } catch (Exception e) {
                throw new Exception("\n=====================================================================================\n"
                        + "--zkHost:" + zkHost + " format error ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            }

            //获取配置参数
            //获取并检查parentNode
            parentNode = confMap.get("parentNode");
            if (parentNode == null || parentNode == "")
                throw new Exception("\n=====================================================================================\n"
                        + "--parentNode is empty ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            if (!zkClient.exists(parentNode))
                throw new Exception("\n=====================================================================================\n"
                        + "--it's not exist parentNode in zkNodes: " + parentNode + ",please check your flume conf file!!!\n"
                        + "=====================================================================================\n");

            //获取并检查monitorPort
            monitorPort = confMap.get("monitorPort");
            if (monitorPort == null || monitorPort == "")
                throw new Exception("\n=====================================================================================\n"
                        + "--monitorPort is empty ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");

            try {
                Integer.parseInt(monitorPort);
            } catch (NumberFormatException e) {
                throw new Exception("\n=====================================================================================\n"
                        + "--monitorPort:" + monitorPort + " format error ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            }

            //获取并检查receiver
            receiver = confMap.get("receiver");
            if (receiver == null || receiver == "")
                throw new Exception("\n=====================================================================================\n"
                        + "--receiver is empty ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            try {
                String[] receivers = receiver.split(";");
                for (String str : receivers) {
                    if (!str.contains("@") || !str.contains(".com")) {
                        throw new Exception();
                    }
                }
            } catch (Exception e) {
                throw new Exception("\n=====================================================================================\n"
                        + "--receiver: " + receiver + " format error ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            }

            //获取并检查flumeWatchCycle,可以允許為空，採取默認週期進行監控
            flumeWatchCycle = confMap.get("flumeWatchCycle");
            if (flumeWatchCycle != null && flumeWatchCycle != "") {
                int mod;
                try {
                    mod = Integer.parseInt(flumeWatchCycle) % 60000;//线程休眠为long mis
                } catch (NumberFormatException e) {
                    throw new Exception("\n=====================================================================================\n"
                            + "--flumeWatchCycle: " + flumeWatchCycle + " format error ,please check your flume conf file!!!\n"
                            + "=====================================================================================\n");
                }
                if (mod != 0)
                    throw new Exception("\n=====================================================================================\n"
                            + "--flumeWatchCycle: " + flumeWatchCycle + " must be a multiple of 60000  ,please check your flume conf file!!!\n"
                            + "=====================================================================================\n");
            } else
                flumeWatchCycle = 60000 + "";

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }


        //检测confName是否已经使用
        if (confMap.get("confName") == null || confMap.get("confName") == "")
            try {
                throw new Exception("\n=====================================================================================\n"
                        + "--confName is empty ,please check your flume conf file!!!\n"
                        + "=====================================================================================\n");
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        else {
            confName = confMap.get("confName");
            List<String> children = zkClient.getChildren(parentNode);
            Map currentNodeMap = new HashMap<String, String>();
            if (children != null) {
                for (String node : children) {
                    String nodePath = parentNode + "/" + node; //拿到node在zookeeper中的路径
                    String zkData = zkClient.readData(nodePath).toString();
                    JsonObject returnData = new JsonParser().parse(zkData).getAsJsonObject();
                    currentNodeMap = gson.fromJson(returnData, HashMap.class);
                    try {
                        if (confName.equals(currentNodeMap.get("confName"))) {
                            throw new Exception("\n=====================================================================================\n"
                                    + "--already exists confName in watching,please check flume conf file; 重复的confName!!!\n"
                                    + "=====================================================================================\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(0);
                    }
                }
            }
        }


        //将信息封装到map装成json传输到zk
        eventMap = new HashMap<String, String>();
        //获取并检查currentHost
        try {
            eventMap.put("currentHost", InetAddress.getLocalHost().getHostAddress().toString());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        eventMap.put("confName", confName);
        eventMap.put("receiver", receiver);
        eventMap.put("monitorPort", monitorPort);
        eventMap.put("flumeWatchCycle", flumeWatchCycle);
        zkData = gson.toJson(eventMap);

        String create = zkClient.create(parentNode + "/" + confName, zkData, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public Event intercept(Event event) {

        return event;
    }

    public List<Event> intercept(List<Event> events) {
        eventBody=new StringBuilder();
        //System.out.println("new eventBody:"+eventBody.toString());
        keyValues=new StringBuilder();
       // System.out.println("new keyValues:" + keyValues.toString());
        jsonParser = new JsonParser();
        results = new ArrayList<Event>();

        //获取event，对每个event进行处理。
        for (Event event : events) {
            // System.out.println("event:"+new String(event.getBody()));
            // System.out.println("=====================================================");
            eventBody.append(new String(event.getBody()));
            //System.out.println("eventBodytest:"+eventBody.toString()+";count:"+count++);
            returnData = jsonParser.parse(eventBody.toString()).getAsJsonObject();
            bodyMap = gson.fromJson(returnData, LinkedHashMap.class);

            if (!bodyMap.containsKey("SUBSTRATE_ID") ||
                    !bodyMap.containsKey("TRACE_DTTS")
                    || !bodyMap.containsKey("EQP_MODULE_ID")) {
                throw new RuntimeException("缺少主键字段：" + bodyMap.toString());
            }

            if ((bodyMap.get("op_type").toString().toUpperCase().contains("I")) &&
                    (bodyMap.get("EQP_MODULE_ID").toString().toUpperCase().contains("CSOT2")) &&
                    (bodyMap.get("EQP_MODULE_ID").toString().toUpperCase().contains("ARRAY")) &&
                    (bodyMap.get("EQP_MODULE_ID").toString().toUpperCase().contains("TBMSP400")) &&
                    ("TB546A7AB000".equals(bodyMap.get("PRODUCT_ID"))) &&
                    ("3200".equals(bodyMap.get("OPERATION_ID")))) {

                System.out.println("event:" + new String(event.getBody()));
                System.out.println("=====================================================");

            for (Object obj : bodyMap.keySet()) {
                if ("SUBSTRATE_ID".equals(obj)) {
                    keyValues.append("`GLASS_ID`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    continue;
                }
              /*  if ("op_type".equals(obj)) {
                    keyValues.append("`op_type`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    continue;
                }
                if ("LOT_TYPE_CD".equals(obj)) {
                    keyValues.append("`LOT_TYPE`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    continue;
                }*/
                if ("EQP_MODULE_ID".equals(obj)) {
                    //keyValues.append("`EQP_MODULE_ID`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    keyValues.append("`EQP_ID`" + "=" + "'" +  bodyMap.get(obj).toString().substring(bodyMap.get(obj).toString().lastIndexOf("/")+1) + "'" + ",");
                    continue;
                }
                /* if ("PRODUCT_ID".equals(obj)) {
                    keyValues.append("`PRODUCT_ID`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    continue;
                }
                if ("OPERATION_ID".equals(obj)) {
                    keyValues.append("`OPERATION_ID`" + "=" + "'" + bodyMap.get(obj) + "'" + ",");
                    continue;
                }*/
                if ("TRACE_DTTS".equals(obj)) {
                    keyValues.append("`EVENT_TIME`" + "=" + "'" + bodyMap.get(obj) /*+ "_" + UUID.randomUUID().toString()*/ + "'" + ",");
                    continue;
                }
                //处理FILE_DATA_1的格式，并解析其中的参数
                if ("FILE_DATA_1".equals(obj)) {
                    arr = bodyMap.get(obj).toString().split("\\t");
                    for (String str : arr) {
                        split = str.split("=");

                        if (!"NAN".equals(split[1].split("@")[0].trim().toUpperCase())) {
                            keyValues.append(
                                    "`" + split[0] + "`"
                                            + "=" + "'" + split[1].split("@")[0].trim() + "'" + ","
                            );
                        } else {
                            continue;
                        }
                    }
                }
            }
        }else{
            keyValues.setLength(0);
            eventBody.setLength(0);
            continue;
        }

            if (keyValues.length()>1){
                keyValues.replace(0,keyValues.length(),keyValues.substring(0,keyValues.lastIndexOf(",")));
                event.setBody(keyValues.toString().getBytes());
               //System.out.println("====================================");
                //System.out.println("body:{"+keys+":"+values+"} count:"+count+"size:"+bodyMap.size());
              // System.out.println(new String(event.getBody()).toString());
               //System.out.println("====================================");
                //System.out.println(new String(event.getBody()).toString());
                results.add(event);
            }

            keyValues.setLength(0);
            eventBody.setLength(0);
        }

        System.gc();
        return results;
    }

    public void close() {
        System.gc();
    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new Kafka2MysqlIntercepter();
        }

        public void configure(Context context) {
            confMap = context.getParameters();
        }
    }
}