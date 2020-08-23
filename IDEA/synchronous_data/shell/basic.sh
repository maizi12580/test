#!/usr/bin/env bash
desc_tmp='C:\Users\maizi\Desktop\1.txt'
for file1 in $(ls ${desc_tmp})
do
echo $file1
#if($3=="TIMESTAMP"); then
#    {printf "${desc_tmp/$1/tochar'\('$1,\'yyyy-mm--dd hh24:mi:ss\''\)'}" fs ","}
#fi
done
#统计每个库有几张表，每张表有几行
function totalTable()

#break必须分开单独一行语句,否则不奏效,
function demoFun(){
while :
do
    read num
    case $num in
        1) echo "select 1"
            continue ;;
        2) echo "select 2" ;;
        *) echo "no 1 and 2"
            break
        ;;
    esac
done
}
#函数必须先写好然后才可以调用
demoFun

#循环,注释 -e为开启转义 \n换行 \c不换行 shell 变量调用需要加 $,但是 for 的 (()) 例如for((i=1;i<=5;i++));do
:<<EOF
for file1 in $(ls D:/QQ)
do
    echo -e "1:--- \n" $file1
    for file2 in `ls D:/QQ`
        do
            echo -e "2--- \n" $file2
        done
done
EOF

:<<EOF
#字符串中,双引号可以拼接加转义字符,单引号不可以,只可以做引用
your_name="runoob"
greeting="hello,"$your_name" !"
greeting_2='hello,'$your_name' !'
echo $greeting  $greeting_2
#字符串长度
echo ${#your_name}
#查找子字符串
echo `expr index "$greeting" h`
EOF

:<<EOF
array_name=(value value1 value22 value333)
length1=${#array_name[@]}
length2=${#array_name[*]}
# 取得数组单个元素的长度
echo ${array_name[@]} "$length1" $length2
for i in ${array_name[*]}
do
    #输出元素
    echo $i
    #数组元素的长度
    echo ${#i}
done
EOF

:<<EOF
val=`expr 2 + 2`
echo $val
a=10 b=20 c=10
# -eq是等于 -ne是不等于 -gt是大于 -lt是小于 -ge是大于等于 -le是小于等于 -o是或 -a是与 !是布尔非
if [ $a -eq $b -o $a -eq $b ];
    then echo "a=b"
    else echo "a!=b"
fi
if [ $a -eq $c ];
    then echo "a=c"
fi
#使用&&和||必须用[[]]
if [[ $a -lt 100 || $b -gt 100 ]]
then
   echo "返回 true"
else
   echo "返回 false"
fi
#字符串: =是相等,!=是不相等,-z长度为零为true,-n不为零为true,$ 字符串不为空为true
a="abc"
b="efg"
if [ $a = $b ]
then
   echo "$a = $b : a 等于 b"
else
   echo "$a = $b: a 不等于 b"
fi
EOF

:<<EOF
#-r可读返回true -w返回true -x可执行返回true -s不为空返回true -e存在返回true
file = "D:/QQ/Uninstall.xml"
if [ -r $file ];
then
   echo "文件可读"
else
   echo "文件不可读"
fi
EOF

