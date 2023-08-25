# LibzdbForDM
libzdb适配达梦数据库
达梦数据库提供的c语言的接口使用的是句柄的形式，缺少一个有效的封装，很难用。而libzdb本身提供了一套统一的接口，具体的可以查看libzdb相关的介绍。
libzdb适配达梦数据库，实现连接池、sql语句执行的接口,提供方便的接口调用。
目前基本的增删改查已经实现，细节的bug目前还需要继续查。
暂时没有详细的教程，还有其他的开发工作需要进行，所以预计一个月左右有时间更新出来。如果急需可以私我邮箱2497213126@qq.com
同时欢迎一起进行适配工作，一起debug



测试环境含有mysql数据库，执行configure脚本后会自动检测当前所包含的数据库环境，生成相应的makefile文件。由于我这边为mysql的环境，所以需要将makefile中的相关mysql的驱动路径修改为达梦的dmdci驱动
//此路径下包含DCI.h和DCI1.h  
DBCPPFLAGS =  -I/home/dyy/dci/include
//将dmdci驱动复制到此路径下，并进行引用
DBLDFLAGS =  -L/usr/lib/x86_64-linux-gnu -lpthread -ldl -lssl -lcrypto -lresolv -lm -lrt -ldmdci
根据上述两个路径，查看makefile文件中的变量设置，都进行修改
