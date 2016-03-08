<?php 

	require("../importer.php");

	$importer = new Importer();

	$importer->period = "halfhour";

	$importer->importFilesToDb("mytho");

?>