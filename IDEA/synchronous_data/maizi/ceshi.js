start()

function start() {
    //id的 5dd64db3ad11d52376000009
    var timesUTC = '516a76a1c9565daf06030000'
    var times = timesUTC.substr(0,7)
    var date = new Date(times);
    console.log(date.getHours()+":"+date.getMinutes());
}
function start1() {
    var date = new Date();
    var oldTimestamp = Date.parse(date)
    console.log(oldTimestamp)

    var date1 = new Date();
    date1.setFullYear(date1.getFullYear()-1)
    var oldTimestamp1 = Date.parse(date1)
    console.log(oldTimestamp1);

    var date2 = new Date();
    // date2.setFullYear(date2.getFullYear()-1)
    var oldTimestamp2 = Date.parse(date2) - (365*24+8)*60*60*1000
    console.log(oldTimestamp2);

    var date3 = new Date();
    date3.setFullYear(date3.getFullYear()-1)
    date3.setHours(date3.getHours()-8)
    console.log(date3)
    var oldTimestamp3 = Date.parse(date3)
    console.log(oldTimestamp3);
}


