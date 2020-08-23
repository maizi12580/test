
#!/bin/sh
#配置集群机器中互相免密
file="
T-NL-HISDB01
T-NL-HISDB02
T-NL-HISDB03"
for hostname in `cat ${file}`
do
	ssh ${hostname} ssh-keygen -t rsa  
done

for hostname in `cat ${file}`
do
	ssh ${hostname} cat /home/sdbadmin/.ssh/id_rsa.pub>>/home/sdbadmin/.ssh/authorized_keys
	#将${hostname}中的ssh文件追加到authorized_keys
	echo "${hostname}"
done
for hostname in `cat ${file}`
do	
	scp /home/sdbadmin/.ssh/authorized_keys ${hostname}:/home/sdbadmin/.ssh/ 
	echo "${hostname}"
	#将追加到的密码指令拷贝到各个机器上
done  