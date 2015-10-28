<?php

$client = "client01";
$key = "1ZYw71APsQ";

function signature()
{
    global $client,$key;
    $string = array();
    $string["client"] = $client;
    $string["nonce"] = rand(1000,10000);
    $string["timestamp"] = time();
    $list = array($key, $string["timestamp"], $string["nonce"]);
    sort($list,SORT_STRING);
    print_r($list);
    $string["signature"] = sha1(join("",$list));
    return $string;
}

$str = signature();
$out = [];
foreach ($str as $k => $v){
    array_push($out,"$k=$v");
}

print join("&",$out);