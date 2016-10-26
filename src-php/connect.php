<?php

$m = new MongoDB\Driver\Manager('mongodb://localhost:27017');

$filter = array();
$options = [
   "projection" => array(
        "text" => 1,
        "_id" => 0,
    ),
 ];


$query = new MongoDB\Driver\Query($filter, $options);

$rows = $m->executeQuery('marie.twitters', $query); // $mongo contains the connection object to MongoDB
foreach($rows as $r){
   if (strpos($r->text, 'Bajala y participá') !== false) {
    	echo $r->text;
   		echo "<br>";
	}
}

 
?>

