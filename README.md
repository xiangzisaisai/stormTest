# stormTest
storm test
参考
开发Storm的第一步就是设计Topology，为了方便开发者入门，首先我们设计一个简答的例子，该例子的主要的功能就是把每个单词的后面加上Hello，World后缀，然后再打印输出，整个例子的Topology图如下：

TestWordSpout ————>  ExclamationBolt ————> PrintBolt

整个Topology分为三部分：
　　TestWordSpout:数据源，负责发送words
　　ExclamationBolt:负责把每个单词后面加上后缀
　　PrintBolt:负责把单词打印输出
