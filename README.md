# kafka2mysql
1.将自定义的interceptor与sink打包放入fdc2mysql.sh中jar所指定的目录下</br>
2.启动fdc2mysql.sh即可</br>
3.注意:&nbsp;&nbsp;1.http监控的sh与properties文件中的port要保持一致</br>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.关于source，channel，sink端的batch的设置，</br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;source batch <= channel(tanstion batch) <= sink batch(mysql batch)</br>
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 3.所使用的是kafka source及channel 如果需要从头消费数据，需要将kafka source 和channel</br>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; 的consumer topic和topic分别重置名，否则不会生效，至于source还是从offset的位置开始消费</br>
       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;而channel中还会残留数据</br>
4.至于已启动的进程存在于104服务器，且已经加入监控</br>
