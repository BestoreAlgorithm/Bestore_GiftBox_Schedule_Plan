<?php
//输出之前缓冲
ob_end_clean();
header("Connection: close");
header("HTTP/1.1 200 OK");
ob_start();#开始当前代码缓冲

header('Content-Type:application/json');
// 指定允许被访问的域名
header('Access-Control-Allow-Origin:*');  
// 响应类型  
header('Access-Control-Allow-Methods:*');  
// 响应头设置  
header('Access-Control-Allow-Headers:x-requested-with,content-type'); 


//接受传递的json数据
$content=file_get_contents ( 'php://input' );
//$content=2;

//判断json文件是否非空
function json_empty(){

	//存储json文件名的数组
	$file_name=['priority','capacity','boms','calendar'];

	for($i=0;$i<4;$i++){
			$js_content=file_get_contents('jsons/'.$file_name[$i].'.json');
			if($js_content==''){
				return false;
			}
	}
	return true;	
}


//发送请求回调结果数据
function postData($url,$data){

    $str = 'application/json';

    $options['http'] = array(
        'timeout' => 10,
        'method'  => 'POST',
        'header'  => "Content-Type: $str;charset=utf-8",
        'content' => $data,
    );
    $context = stream_context_create($options);
    return file_get_contents($url, false, $context);
}


//传输数据非空
if($content==''){

	$data=array('code' => 404, 'msg' => 'Do not transmit empty data!');
	echo(json_encode($data));
}
else{
	if(!is_dir('jsons')){
		//不存在该文件夹时则自动创建
		mkdir('jsons');
	}

	if (!json_empty()){
		//基础数据为空时，返回状态码403
		$data=array('code' => 403, 'msg' => 'Please transmit other needed information!');
		echo(json_encode($data));
		}
	else{
		//给结果返回参数
		$data=array('code' => 200, 'msg' => 'success');
		echo (json_encode($data));

		$size=ob_get_length();
		header("Content-Length: $size");	
		ob_end_flush(); // 输出当前缓冲
		flush(); // 输出PHP缓冲
		ignore_user_abort(true); // 后台运行，这个只是运行浏览器关闭，并不是直接就中止返回200状态。
		set_time_limit(0); // 取消脚本运行时间的超时上限
		
		//将传输数据保存至ProducePlan.json中
		file_put_contents('jsons/PackPlan.json', $content);
		//执行py文件并获得返回信息
		exec("/var/www/anaconda3/bin/python3 /var/www/html/13weeks.py",$arry,$info);
		//读取产生的结果文件
		$js_result=json_encode(json_decode(file_get_contents('13Weeks_result.json')));
	}

	
	//$url='http://10.28.113.228:8080/v1/plan/planningAndScheduling';
	//$url='http://10.28.180.73:8080/v1/plan/packingPlanRest';
	$url='http://10.28.180.73:8080/v1/plan/planningAndScheduling';
	//$url='http://127.0.0.1/post.php';
	//$url='http://127.0.0.1/1.php';
	
	//发送请求，回调结果
	$output=postData($url,$js_result);
	echo $output;
	
}


?>

