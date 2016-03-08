<?php

	require("../importer.php");

	$importer = new Importer();

	$importer->download("icecat");
	$importer->convertGzip("icecat.gz");
	$importer->scanIcecat();

?>