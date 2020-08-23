var len =10;
for(var i=1;i < 10; i++){
    //console.log(getPhone())
    // console.log(getName())
    //console.log(getBankAccount())
}
/*获取随机手机号*/
function getPhone(){
    var randomCode = function (lens,nums) {
        for (var i = 0,rs = ''; i < lens; i++)
            rs += nums.charAt(Math.floor(Math.random() * nums.length ) );
        return rs;
    };
    var randomPhoneNumber = function(){
        // 第1位是1 第2,3位是3458 第4-7位是* 最后四位随机
        return [1,randomCode(2,'34568'),randomCode(8,'0123456789')].join('');
    };
    return randomPhoneNumber();
}

/*获取随机名字*/
function getName(){
    var familyName = ['李','王','张','刘','陈','杨','赵','黄',
            '徐','孙','胡','朱','高','林','何','郭',
            '马','罗','梁','宋','郑','谢','韩','唐',
            '冯','于','董','萧','程','柴','袁','邓',
            '许','傅','沈','曾','彭','吕','苏','卢',
            '蒋','蔡','贾','丁','魏','薛','叶','阎',
            '余','潘','杜','戴','夏','钟','汪','田'],
        secondName = ["子","凡","悦","思","奕","易","坚","莎","耘","国",
            "仑","良","裕","融","致","德","卿","桂","钊","钧",
            "铎","铭","皑","柏","镇","淇","淳","一","洁","茹",
            "清","吉","克","先","依","浩","亮","允","元","源",
            "渊","和","函","妤","宜","云","琪","菱","宣","沂",
            "健","信","媛","凯","欣","可","洋","萍","荣","榕",
            "含","佑","明","雄","芝","英","义","卿","乾","亦",
            "雅","芝","萱","昊","芸","天","岚","昕","尧","鸿",
            "棋","琳","宸","乔","丞","安","毅","凌","惠","珠",
            "泉","坤","恒","渝","菁","龄","弘","佩","勋","宁",
            "元","栋","嘉","哲","俊","博","维","泰","乐","佳"];
    var fNameLen = familyName.length,
        sNamelen = secondName.length;
    var mName ="",len = Math.floor(Math.random()*3)+2;
    if(len==2){
        mName = familyName[Math.floor(Math.random()*fNameLen)]
            + secondName[Math.floor(Math.random()*sNamelen )];
    }else{
        mName = familyName[Math.floor(Math.random()*fNameLen )]+secondName[Math.floor(Math.random()*sNamelen )]
            + secondName[Math.floor(Math.random()*sNamelen )];
    }
    return mName;
}
/*生成随机银行卡号*/
function getBankAccount(){
    var bank_no = Math.floor(Math.random()*16);
    var prefix = "";
    switch (bank_no) {
        case 0:
            prefix = "622202";
            break;
        case 1:
            prefix = "622848";
            break;
        case 2:
            prefix = "622700";
            break;
        case 3:
            prefix = "622262";
            break;
        case 4:
            prefix = "621661";
            break;
        case 5:
            prefix = "622666";
            break;
        case 6:
            prefix = "622622";
            break;
        case 7:
            prefix = "622556";
            break;
        case 8:
            prefix = "622588";
            break;
        case 9:
            prefix = "622155";
            break;
        case 10:
            prefix = "622689";
            break;
        case 11:
            prefix = "622630";
            break;
        case 12:
            prefix = "622908";
            break;
        case 13:
            prefix = "621717";
            break;
        case 14:
            prefix = "622323";
            break;
        default:
            prefix = "622309";
            break;
    }
    for (var j = 0; j < 13; j++) {
        prefix = prefix + Math.floor(Math.random()*10);
    }
    return prefix;
}
