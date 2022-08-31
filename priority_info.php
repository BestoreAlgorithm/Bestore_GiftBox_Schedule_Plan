<?php
header('Content-Type:application/json');
// 指定允许被访问的域名
header('Access-Control-Allow-Origin:*');  
// 响应类型  
header('Access-Control-Allow-Methods:*');  
// 响应头设置  
header('Access-Control-Allow-Headers:x-requested-with,content-type'); 

//接受传递的json数据
$content=file_get_contents ( 'php://input' );
if($content!=''){
	if(!is_dir('./jsons')){
			mkdir('./jsons');
		}
	file_put_contents('./jsons/priority.json', $content);
	$data = array('code' => 200, 'msg' => 'success');

}
else{
	$data = array('code' => 404, 'msg' => 'Do not transmit empty data!');
	}
//$content_json=json_decode($content,true);
echo json_encode($data);

?>
