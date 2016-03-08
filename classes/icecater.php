<?php 

	class icecater {

		const DEBUG = false;

		function __construct($args = null) 
		{
			$this->args = $args;
			$this->db = helper::getPDOHandle();
			if (self::DEBUG) self::debugBlock();
		}

		public function actionIcecat() 
		{

			$target = $this->args[0];

			$found = 0;
			$counter = 0;

			if ($target) {

				if ($target == "all") {
					// TODO: UNION OF ALL FLAT TABLES AND START CHECKING CHIT OUT
					helper::writeLine("This block of the script hasn't been developed yet! ",helper::ERROR_ALERT,helper::DIE_AFTER_MSG,helper::LOG_MSG);
				}

				else {
	
					$target_table = "products_".$this->args[0]."_flat";

					if (helper::tableExists($this->db, $target_table)) {
						$query = $this->db->prepare("SELECT ".$target_table.".id, ".$target_table.".EAN, ".$target_table.".part_number, mytho_produttori.des as manufacturer
									 				 FROM ".$target_table."
													 LEFT JOIN mytho_produttori ON (".$target_table.".manufacturer_id = mytho_produttori.cod)
													 LEFT JOIN icecat ON (icecat.id = ".$target_table.".id)
													 WHERE icecat.id IS NULL");
					}
					else {
						helper::writeLine("The table $target_table doesn't exists! ",helper::ERROR_ALERT,helper::DIE_AFTER_MSG,helper::LOG_MSG);
					}
				}
				
				$query->execute();
				$product_list = $query->fetchAll(PDO::FETCH_ASSOC);

				$query = $this->db->prepare("SELECT username, password 
											FROM system_sources WHERE name = 'icecat'");

				$query->execute();
				$result = $query->fetch(PDO::FETCH_ASSOC);

				foreach ($product_list as $product) {

					$url = array();
					$url_found = null;

					$url["ean"] = "http://".$result['username'].":".$result['password']."@data.icecat.biz/xml_s3/xml_server3.cgi?ean_upc=".$product['EAN'].";lang=it;output=productxml";

					$manufacturers = explode("/",$product['manufacturer']);
					$url["pn"] = array();

					foreach ($manufacturers as $manufacturer) {
						$url["pn"][] = "http://".$result['username'].":".$result['password']."@data.icecat.biz/xml_s3/xml_server3.cgi?prod_id=".$product['part_number'].";vendor=".$manufacturer.";lang=it;output=productxml";
					}

					if ($product['EAN']) {
					
						$xml = @simplexml_load_file($url['ean']);
						$url_found = ($xml->Product['Code'] == "1") ? $url['ean'] : false;
					}

					if (!$url_found) {

						if ($product['part_number'] && $product['manufacturer']) {

							$k = 0;

							do {
								
								$xml = @simplexml_load_file($url['pn'][$k]);
								$url_found = ($xml->Product['Code'] == "1") ? $url['pn'][$k] : false;
								$k++;

							} while (!$url_found && $k < count($url['pn']));

						}
					}

					if ($url_found) {

						$url_icecat = str_replace("http://".$result['username'].":".$result['password']."@","",$url_found);
						$high_pic = addslashes($xml->Product['HighPic']);
						$low_pic = addslashes($xml->Product['LowPic']);
						$thumb_pic = addslashes($xml->Product['ThumbPic']);
						$long_desc = addslashes($xml->Product->SummaryDescription->LongSummaryDescription);
						$short_desc = addslashes($xml->Product->SummaryDescription->ShortSummaryDescription);

						$query = $this->db->prepare("REPLACE INTO icecat (`id`,`path`,`high_pic`,`low_pic`,`thumb_pic`,`long_desc`,`short_desc`) VALUES ('".$product['id']."','".$url_icecat."','".$high_pic."','".$low_pic."','".$thumb_pic."','".$long_desc."','".$short_desc."')");

						$query->execute();

						$found++;

					}
					
					$counter++;

					echo "found: ".$found."/".$counter." Percentage ".(float)($found/$counter*100)."% using ".json_encode($product)." part_number_url = ".count($url['pn'])."\n";

				}
			}

			else {
				self::help();
			}
		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			// helper::writeLine(E_SYNC_ROOT." ".ICECATER_ROOT." icecat all\t\t\t\tFinds icecat links for all flat tables");
			helper::writeLine(E_SYNC_ROOT." ".ICECATER_ROOT." icecat [flat_table]\t\t\tFinds icecat links for [flat_table]");
			// helper::writeLine(E_SYNC_ROOT." ".ICECATER_ROOT." icecat [flat_table] [manufacturer]\tFinds icecat links for [flat_table] and [manufacturer]");
			helper::writeLine(helper::getSeparator());
		}

		/**
		* Block method inserted in the class constructor to debug on the fly a method or a variable 
		* @param 	void
		* @return	void
		*/
		private function debugBlock() 
		{
			helper::writeLine("Debug Mode",helper::SYSTEM_ALERT);
			// insert here an output
			$xml = file_get_contents("http://newlinexml:testxml@data.icecat.biz/xml_s3/xml_server3.cgi?prod_id=9S7-175912-255;vendor=MSI;lang=it;output=productxml");
			echo ($xml);
			die();
		}
	}

?>