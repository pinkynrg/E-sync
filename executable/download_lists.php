<?php

	require("../importer.php");

	$importer = new Importer();

	$importer->download("mytho");
	$importer->download("esprinet");

	$importer->convert();

?>