<?php 

	require("../importer.php");

	$importer = new Importer();

	$importer->period = "day";

	$importer->importFilesToDb("mytho");
	$importer->importFilesToDb("esprinet");

?>