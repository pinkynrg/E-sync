<?php 

require_once("db_access.php");

class Importer {

	public function __construct() {
		ini_set("auto_detect_line_endings", true);  // recognize line ending in mac computers
		ini_set("memory_limit",-1);
		set_time_limit(0);

		$this->db = new PDO('mysql:host='.DB_HOST.';dbname='.DB_NAME.';charset=utf8', DB_USER, DB_PASS);
		$this->db->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING );

		$this->log_handle = fopen($this->log,"a+"); // opens log file for writing
	}

	// hosts for resective sources
	private $host = array(
		"ingram" => "ftpsecure.ingrammicro.com",
		"ingram2" => "ftp.ingrammicro.it",
		"cgross" => "212.19.96.117",
		"ellegi2" => "2.228.91.2",
		"icecat" => "data.icecat.biz",
		"esprinet" => "dataservice.esprinet.com"
	);

	// users for respective sources
	private $user = array(
		"ingram" => "317486",
		"ingram2" => "IT_SIAE",
		"cgross" => "21453",
		"ellegi2" => "Lg2",
		"icecat" => "newlinexml",
		"esprinet" => "1-099550"
	); 

	// passwords for respective sources
	private $password = array(
		"ingram" => "CPR555",
		"ingram2" => "SIAE",
		"cgross" => "6fuqlhfs",
		"ellegi2" => "!lutg2!x",
		"icecat" => "testxml",
		"esprinet" => "WDLzRduLx1m5do3NRvwj"
	);

	// files to download for each source with name on the server and name to be used when saved locally
	private $files = array(
		"esprinet" => array(
			array(
				"server" => "SchedeP.txt",
				"local" => "esprinet_listino.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"codice" 				=> "varchar(15)",
					"codiceProduttore" 		=> "varchar(35)",
					"codiceEAN" 			=> "varchar(15)",
					"Prod" 					=> "varchar(3)",
					"DescProd" 				=> "varchar(46)",
					"nomeCasaProd" 			=> "varchar(30)",
					"tipo" 					=> "varchar(1)",
					"catMerc" 				=> "varchar(2)",
					"descCatMerc" 			=> "varchar(100)",
					"fam" 					=> "varchar(50)",
					"descFam" 				=> "varchar(100)",
					"grMaster" 				=> "varchar(5)",
					"descGrMaster" 			=> "varchar(35)",
					"dispo" 				=> "int",
					"arrivi" 				=> "int",
					"stato" 				=> "varchar(1)",
					"descrizione" 			=> "varchar(35)",
					"descEstesa" 			=> "varchar(140)",
					"prezzoListino" 		=> "float(10,2)",
					"prezzoRivenditore" 	=> "float(12,2)",
					"scontoDealerStandard" 	=> "float(7,2)",
					"prezzoPromo" 			=> "float(7,2)",
					"dataPromoDa" 			=> "date",
					"dataPromoA" 			=> "date",
					"LinkScheda" 			=> "varchar(250)",
					"pesoLordo" 			=> "int",
					"altezza" 				=> "int",
					"lunghezza" 			=> "int",
					"profondita" 			=> "int",
					"raee" 					=> "float(7,2)",
					"modello" 				=> "varchar(160)",
					"tempoDOAEsprinet" 		=> "int",
					"tempoGaranziaEsprinet" => "int",
					"tempoDOAEndUser" 		=> "int",
					"tempoGaranziaEndUser" 	=> "int",
					"streetPrice" 			=> "float(10,2)",
					"qtaMinimaOrd" 			=> "int"
				),
				"indices" => array(
					"codiceProduttore, Prod" => "unique",
					"catMerc" => ""
				)
			)
		),
		"icecat" => array(
			array(
				"server" => "/export/level4/it/files.index.csv.gz", // aggiornato ogni giorno
				"local" => "icecat3.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"path" 				=> "varchar(200)",
					"product_id" 		=> "varchar(200)",
					"updated" 			=> "varchar(200)",
					"quality" 			=> "varchar(200)",
					"supplier_id" 		=> "varchar(200)",
					"prod_id" 			=> "varchar(200)",
					"catid" 			=> "varchar(200)",
					"m_prod_id" 		=> "varchar(200)",
					"ean_upc" 			=> "varchar(200)",
					"on_market" 		=> "varchar(200)",
					"country_market" 	=> "varchar(200)",
					"model_name" 		=> "varchar(200)",
					"product_view" 		=> "varchar(200)",
					"high_pic" 			=> "varchar(200)",
					"high_pic_size" 	=> "varchar(200)",
					"high_pic_width" 	=> "varchar(200)",
					"high_pic_height" 	=> "varchar(200)",
					"m_supplier_id" 	=> "varchar(200)",
					"m_supplier_name" 	=> "varchar(200)"
				)
			)
		),
		"ingram" => array(
			array(
				"server" => "/FUSION/IT/S2194/PRICE.ZIP", // aggiornato ogni giorno alle 23:00
				"local" => "ingram_listino.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"action_indicator" 			=> "varchar(1)",
					"ingram_part_number" 		=> "varchar(12)",
					"vendor_number" 			=> "varchar(4)",
					"vendor_name" 				=> "varchar(36)",
					"ingram_part_description_1" => "varchar(31)",
					"ingram_part_description_2" => "varchar(35)",
					"retail_price" 				=> "float(7,2)",
					"vendor_part_number" 		=> "varchar(20)",
					"weight" 					=> "float(7,2)",
					"upc_code" 					=> "varchar(13)",
					"length" 					=> "float(7,2)",
					"width" 					=> "float(7,2)",
					"height" 					=> "float(7,2)",
					"price_change_flag" 		=> "varchar(1)",
					"custom_price" 				=> "float(7,2)",
					"special_price_flag" 		=> "varchar(1)",
					"availability_flag" 		=> "varchar(1)",
					"status" 					=> "varchar(1)",
					"alliance_flag" 			=> "varchar(1)",
					"cpu_code" 					=> "varchar(6)",
					"media_type" 				=> "varchar(4)",
					"ingram_cat_subcat" 		=> "varchar(4)",
					"new_item_receipt_flag" 	=> "varchar(1)",
					"country_code" 				=> "varchar(2)"
				),
				// "pkeys" => array(
				// 	"ingram_part_number"
				// )
				"indices" => array(
					"ingram_part_number" => "unique"
				)
			),
			array(
				"server" => "/FUSION/IT/S2194/PRICE1.ZIP", // aggiornato ogni giorno alle 23:00
				"local" => "ingram_listino_light.csv",
				"period" => "halfhour",
				"update_type" => "update",
				"flag" => "action_indicator",
				"header" => array(
					"action_indicator" 			=> "varchar(1)",
					"ingram_part_number" 		=> "varchar(12)",
					"vendor_number" 			=> "varchar(4)",
					"vendor_name" 				=> "varchar(36)",
					"ingram_part_description_1" => "varchar(31)",
					"ingram_part_description_2" => "varchar(35)",
					"retail_price" 				=> "float(7,2)",
					"vendor_part_number" 		=> "varchar(20)",
					"weight" 					=> "float(7,2)",
					"upc_code" 					=> "varchar(13)",
					"length" 					=> "float(7,2)",
					"width" 					=> "float(7,2)",
					"height" 					=> "float(7,2)",
					"price_change_flag" 		=> "varchar(1)",
					"custom_price" 				=> "float(7,2)",
					"special_price_flag" 		=> "varchar(1)",
					"availability_flag" 		=> "varchar(1)",
					"status" 					=> "varchar(1)",
					"alliance_flag" 			=> "varchar(1)",
					"cpu_code" 					=> "varchar(6)",
					"media_type" 				=> "varchar(4)",
					"ingram_cat_subcat" 		=> "varchar(4)",
					"new_item_receipt_flag" 	=> "varchar(1)",
					"country_code" 				=> "varchar(2)"
				),
				// "pkeys" => array(
				// 	"ingram_part_number"
				// )
				"indices" => array(
					"ingram_part_number" => "unique"
				)
			),
			array(
				"server" => "/FUSION/IT/NEWCATS/NEWCATS.TXT", // aggiornato ogni settimana
				"local" => "ingram_categorie.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"category_description" 	=> "varchar(35)",
					"category_code" 		=> "varchar(2)",
					"subcategory_code" 		=> "varchar(2)"
				),
				// "pkeys" => array(
				// 	"category_code,subcategory_code",
				// )
				"indices" => array(
					"category_code" => "",
					"subcategory_code" => ""
				)
			),
			array(
				"server" => "/FUSION/IT/AVAIL/TOTITHRL.ZIP", // aggiornato ogni 60 minuti 
				"local" => "ingram_disponibilita.csv",
				"period" => "halfhour",
				"update_type" => "drop_and_create",
				"header" => array(
					"ingram_part_number" 	=> "varchar(12)",
					"vendor_number" 		=> "varchar(20)",
					"availability" 			=> "int",
					"ordered" 				=> "int",
					"arrival_date" 			=> "date"
				),
				// "fkeys" => array(
				// 	"ingram_part_number"	=> "ingram_listino(ingram_part_number)"
				// ) 
				"indices" => array(
					"ingram_part_number" => "unique"
				)
			)
		),
		"cgross" => array(
			array(
				"server" => "listini.zip", // aggiornato ogni giorno (irregolare)
				"local" => "cgross_listino.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"agenzia" 					=> "varchar(20)",
					"articolo" 					=> "varchar(20)",
					"descrizione" 				=> "varchar(50)",
					"produttore" 				=> "varchar(12)",
					"prezzo_listino" 			=> "float(7,2)",
					"prezzo_esposto" 			=> "float(7,2)",
					"prezzo_netto" 				=> "float(7,2)",
					"immediata" 				=> "int",
					"futura" 					=> "int",
					"codice_macrocategoria"		=> "varchar(5)",
					"macrocategoria"			=> "varchar(20)",
					"codice_categoria" 			=> "varchar(5)",
					"categoria" 				=> "varchar(20)",
					"codice_sottocategoria" 	=> "varchar(5)",
					"sottocategoria" 			=> "varchar(20)",
					"codice_articolo_fornitore" => "varchar(30)",
					"t" 						=> "varchar(1)",
					"***" 						=> "varchar(5)",
					"inizio_promo"			 	=> "date",
					"fine_promo" 				=> "date",
					"contr.siae" 				=> "float(7,2)",
					"contr.raee" 				=> "float(7,2)"
				),
				"indices" => array(
					"codice_articolo_fornitore" => "unique"
				)
			)
		)
		,
		"ingram2" => array(
			array(
				"server" => "ITSKUFEE.TXT", // aggiornato ogni giorno alle 7am
				"local"	=> "ingram_tasse.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"ingram_part_number"		=> "varchar(12)",
					"tassa"						=> "float(7,2)"
				),
				"indices" => array(
					"ingram_part_number"	=> "unique"
				) 
			)
		)
		,
		"ellegi2" => array(
			array(
				"server" => "MAGAZZINO.xls", // aggiornato ogni giorno
				"local" => "ellegi2_listino.csv",
				"period" => "day",
				"update_type" => "drop_and_create",
				"header" => array(
					"codice" 				=> "varchar(16)",
					"descrizione" 			=> "text",
					"prezzo_listino" 		=> "float(7,2)",
					"prezzo_dealer" 		=> "float(7,2)",
					"prezzo_trasferimento" 	=> "float(7,2)",
					"promozione_dealer" 	=> "float(7,2)",
					"disponibilita" 		=> "int(5)",
					"categoria" 			=> "varchar(30)",
					"vendor" 				=> "varchar(20)"
				),
				"indices" => array(
					"codice" => "unique"
				)
			)
		),
		"mytho" => array(
			array(
				"server" => "/mnt/esportazioni_winmytho/EARTMAG.csv", // aggiornato ogni 30 minuti
				"local" => "mytho_listino.csv",
				"period" => "halfhour",
				"update_type" => "drop_and_create",
				"header" => array(
					"CODART" 	=> "varchar(20)",
					"DESART" 	=> "text",
					"UMART" 	=> "varchar(10)",
					"IVART" 	=> "varchar(2)",
					"PREART2" 	=> "float(7,2)",
					"PREART5" 	=> "float(7,2)",
					"PREART6" 	=> "float(7,2)",
					"CODFOR" 	=> "varchar(20)",
					"STATO" 	=> "varchar(1)",
					"COMPAT" 	=> "varchar(50)",
					"PROMO" 	=> "varchar(1)",
					"CPRO" 		=> "varchar(2)",
					"CLIN1" 	=> "varchar(2)",
					"CLIN2" 	=> "varchar(2)",
					"CLIN3" 	=> "varchar(2)",
					"DISP" 		=> "int",
					"DISP4" 	=> "int",
					"PREART4" 	=> "float(7,2)",
					"CATXSC" 	=> "varchar(4)",
					"CATXRI" 	=> "varchar(4)",
					"OFOR" 		=> "int",
					"DTFOR" 	=> "date",
					"PREART1" 	=> "float(7,2)",
					"FLAG" 		=> "varchar(1)",
					"PREART3" 	=> "float(7,2)"
				),
				// "pkeys" => array(
				// 	"CODART"
				// )
				"indices" => array(
					"CODART" => "unique"
				)
			),
			array(
				"server" => "/mnt/esportazioni_winmytho/TBLIN.csv", // aggiornato ogni 30 minuti
				"local" => "mytho_categorie.csv",
				"period" => "halfhour",
				"update_type" => "drop_and_create",
				"header" => array(
					"COD1" 		=> "varchar(2)",
					"COD2" 		=> "varchar(2)",
					"COD3" 		=> "varchar(2)",
					"DES" 		=> "text",
					"CFLAG" 	=> "varchar(1)",
					"CLIS" 		=> "varchar(1)",
					"CPRO" 		=> "varchar(2)",
					"LISWWW" 	=> "varchar(2)"
				),
				"indices" => array(
					"DES" => "fulltext"
				)
			),
			array(
				"server" => "/mnt/esportazioni_winmytho/TBPRO.csv", // aggiornato ogni 30 minuti
				"local" => "mytho_produttori.csv",
				"period" => "halfhour",
				"update_type" => "drop_and_create",
				"header" => array(
					"COD" 		=> "varchar(2)",
					"DES" 		=> "varchar(25)",
					"CODGAM" 	=> "varchar(10)"
				)
			)
		)
	);

	private $final = "products";

	// destination for sources
	private $destination = "../sources";

	// log path 
	private $log = "../log/log.txt";

	private $csved = array();

	private $remove_originals = true;

	private function convertXls($file, $extension) {
	
		require_once 'PHPExcel/Classes/PHPExcel/IOFactory.php';

		$version = $extension == 'xlsx' ? 'Excel2007' : 'Excel5';

		try {

			$reader = PHPExcel_IOFactory::createReader($version);
			$excel = $reader->load($this->destination."/".$file);

			$writer = PHPExcel_IOFactory::createWriter($excel, 'CSV');
			$writer->save($this->destination."/".str_replace($extension, "", $file)."csv");

			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] CONVERTED: \t".$file." to csv\n");

			if ($this->remove_originals) {
				unlink($this->destination."/".$file);
				fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] REMOVED: \t\t".$file."\n");
			}

		} catch (Exception $e) {

			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] !!!ERROR: \tThere was a problem converting ".$file." to csv\n");

		}

	}

	private function convertExtension($file) {
		$old = $this->destination."/".$file;
		$new = str_replace(array('txt','TXT'), 'csv', $old);
		rename($old, $new);
	}

	public function convertGzip($file) {
		$temp = explode(".", $file);
		$file_name = $temp[0].".csv";
		$handle = fopen($this->destination."/".$file_name,"w");
		$gz_handle = gzopen($this->destination."/".$file,"rb");

		while(!gzeof($gz_handle)) {
			fwrite($handle,gzread($gz_handle,4096));
		}

		fclose($handle);
		gzclose($gz_handle);
	}

	private function convertZip($file) {
		$zip = new ZipArchive;
		$res = $zip->open($this->destination."/".$file);
		if ($res === TRUE) {
			$zip->extractTo($this->destination);

			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] UNZIPPED: \t".$file."\n");

			$zip->close();

			if ($this->remove_originals) {
				unlink($this->destination."/".$file);
				fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] REMOVED: \t\t".$file."\n");
			}

		} else {
			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] !!!ERROR: \tThere was a problem unzipping ".$file."\n");
		}
	}

	private function convertCsv($file) {

		// if the csv hasn't been converted then try to do it
		if (!in_array($file, $this->csved)) {

			if ($file == "listini.csv") {

				$content = file($this->destination."/".$file);
				
				// removed file header
				unset($content[0]);
				unset($content[1]);
				unset($content[2]);

				foreach ($content as &$row) {
					
					// substitute comas with dots for prices
					$row = str_replace(",",".",$row);

					// 
					$elems = explode(";",$row);
					
					// trim white spaces
					foreach($elems as &$elem) {
						$elem = trim($elem);
					}

					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;

			}

			if ($file == "PRICE.csv" || $file == "PRICE1.csv") {

				$content = file($this->destination."/".$file);

				foreach ($content as &$row) {
					
					$elems = explode(",",$row);

					foreach($elems as &$elem) {
						$elem = str_replace("\xa0"," ",$elem);
						$elem = trim($elem);
						$elem = preg_replace('!\s+!', ' ', $elem);
					}
				
					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;
			}

			if ($file == "NEWCATS.csv") {

				$content = file($this->destination."/".$file);

				foreach ($content as &$row) {
				
					$row = str_replace('"','',$row);

					$elems = explode(",",$row);

					foreach($elems as &$elem) {
						$elem = trim($elem);
					}
				
					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;

			}

			if ($file == "MAGAZZINO.csv") {

				$content = file($this->destination."/".$file);

				unset($content[0]);

				foreach ($content as &$row) {
				
					$row = str_replace('",','";',$row);

					$elems = explode(";",$row);

					foreach($elems as &$elem) {
						$elem = trim($elem);
						$elem = str_replace(",",".",$elem);
						$elem = str_replace('"','',$elem);
						$elem = preg_replace('!\s+!', ' ', $elem);
					}
				
					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;

			}

			if ($file == "TOTITHRL.csv") {

				$content = file($this->destination."/".$file);

				foreach ($content as &$row) {
				
					$elems = explode(",",$row);

					foreach($elems as &$elem) {
						$elem = str_replace('"','',$elem);
						$elem = trim($elem);
					}
				
					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;
			}

			if ($file == "GIAART.csv" || $file == "EARTMAG.csv" || $file == "ARTMAG.csv" || $file == "TBLIN.csv" || $file == "TBPRO.csv") {

				$content = file($this->destination."/".$file);

				unset($content[0]);

				$content = str_replace(",",".",$content);
				$content = str_replace(";",",",$content);

				file_put_contents($this->destination."/".$file, $content);

				$this->csved[] = $file;
			}

			if ($file == "ITSKUFEE.csv") {
				$content = file($this->destination."/".$file);
				$content = str_replace("     ~883~",",",$content);
				$content = str_replace("IT~","",$content);
				$content = str_replace("~","",$content);
				file_put_contents($this->destination."/".$file, $content);
				$this->csved[] = $file;
			}

			if ($file == "SchedeP.csv") {
				$content = file($this->destination."/".$file);
				unset($content[0]);
				$content = str_replace(",",".",$content);
				$content = str_replace("|",",",$content);

				foreach ($content as &$row) {
				
					$elems = explode(",",$row);

					foreach($elems as &$elem) {
						$elem = trim($elem);
					}
				
					$row = implode(",",$elems)."\r\n";

				}

				file_put_contents($this->destination."/".$file, $content);
				$this->csved[] = $file;
			}

		}

	}

	// copy local files to sources
	private function local($source) {

		foreach ($this->files[$source] as $file) {
			$temp = explode("/",$file['server']);
			$name = $temp[count($temp)-1];
			copy($file['server'], $this->destination."/".$name);
		}

	}

	// downloads remote files from external servers
	private function ftp($source) {
		// set server ftp connection 
		$conn_id = ftp_connect($this->host[$source]);

		// login with username and password
		$login_result = ftp_login($conn_id, $this->user[$source], $this->password[$source]);

		if ($login_result) {

			ftp_pasv($conn_id, true);

			foreach ($this->files[$source] as $file) {
				
				//find original file name from path
				$temp = explode("/",$file['server']);
				$name = $temp[count($temp)-1];

				// try to download $server_file and save to $local_file
				if (ftp_get($conn_id, $this->destination."/".$name, $file['server'], FTP_BINARY)) {
					fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] DOWNLOADED: \t".$file['server']." from $source and saved it as ".$name." in ".$this->destination."\n");
				} else {
					fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] !!!ERROR: \tThere was a problem downloading ".$file['server']." from ".$source."\n");
				}
			}

			ftp_close($conn_id);

		} else {

			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] !!!ERROR: \tIncorrect authentication for ".$source."\n");

		}
	}

	private function http($source) {
		$content = file_get_contents("http://".$this->user[$source].":".$this->password[$source]."@".$this->host[$source].$this->files[$source][0]['server']);
		file_put_contents($this->destination."/icecat.gz", $content);
	}

	private function rename_files() {

		$content = $this->getFileList();

		// renaming single files into readable files such as [source]_[content].csv
		foreach ($content as $old_name) {
			
			// get file name without extention
			$temp = explode(".",$old_name);
			$name = $temp[0];

			foreach ($this->files as $source) {
				foreach ($source as $file) {
					if (strpos($file['server'], $name.".") !== false) {
						rename($this->destination."/".$old_name, $this->destination."/".$file['local']);
						fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] RENAMED: \t\t".$old_name." to ".$file['local']."\n");
					}
				}
			}
		}
	}

	private function getFileList() {

		$content = scandir(dirname(__FILE__)."/sources");

		foreach ($content as $key=>$file) {
			if (is_dir(dirname(__FILE__)."/sources/".$file)) {
				unset($content[$key]);
			}
		}

		// removes some hidden files that has nothing to do with our sources
		$content = array_diff($content, array('.','..','.DS_Store'));

		return $content;
	}

	private function csv_to_array($filename, $header, $delimiter=',') {
		
		$header = array_keys($header);

		if(!file_exists($filename) || !is_readable($filename))
			return FALSE;

		$data = array();
		
		if (($handle = fopen($filename, 'r')) !== FALSE) {
		
			while (($row = fgetcsv($handle, 1000, $delimiter)) !== FALSE) {

				if(!$header)
					$header = $row;
				else
					$data[] = array_combine($header, $row);
			}
			
			fclose($handle);
		}
	
		return $data;
	
	}

	private function strToDate($elem) {

		if (strlen($elem) == 0)
			$elem = "00/00/0000";

		if (strlen($elem) == 18)
			$elem = substr($elem, 0, 10);

		if (count(explode("/",$elem)) == 1) {
			$elem = substr($elem,6,2)."/".substr($elem,4,2)."/".substr($elem,0,4);
		}

		$exploded = explode("/",$elem);
		
		$d = $exploded[0];
		$m = $exploded[1];
		$y = $exploded[2];

		$y_length = strlen($y);

		if ($y_length == 2)
			return "STR_TO_DATE('".$elem."','%d/%m/%y')";
		else
			return "STR_TO_DATE('".$elem."','%d/%m/%Y')";
	}

	private function strToFloat($elem) {
		// $mult = pow(10, 2);
		// return ceil($elem * $mult) / $mult;
		return floatval($elem);
	}

	private function strToInt($elem) {
		if ($elem == '') $elem = 0;
		return $elem;
	}

	private function drop($table_name) {

		$query = $this->db->prepare("set foreign_key_checks = 0");
		$query->execute();

		$query = $this->db->prepare("DROP TABLE IF EXISTS `".$table_name."`;");
		$query->execute();

		$query = $this->db->prepare("set foreign_key_checks = 1");
		$query->execute();

		fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] DROPPED: \t\t$table_name\n");
	}

	private function create($table_name, $header) {

		$i = 0;

		$query = $this->db->prepare("set foreign_key_checks = 0");

		$query_str = "CREATE TABLE `".$table_name."` (";

		foreach ($header as $column => $type) {						// create table
			$query_str .= "`".$column."` ".$type." DEFAULT NULL";
			if ($i<count($header)-1)
				$query_str .= ",";
			else 
				$query_str .= ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

			$i++;
		}

		$query = $this->db->prepare($query_str);
		$query->execute();

		$query = $this->db->prepare("set foreign_key_checks = 1");
		$result = $query->execute();

		if ($result)
			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] CREATED: \t\t$table_name\n");
		else 
			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] !!!ERROR: \tThere was a problem creating ".$table_name."\n");

	}

	private function pkeys($table_name, $pkeys) {

		foreach ($pkeys as $key) {
			$query_str = "ALTER TABLE `".$table_name."` ";
			$query_str .= "ADD PRIMARY KEY (".$key.")";
			$query = $this->db->prepare($query_str);
			$query->execute();
		}
	}

	private function fkeys($table_name, $fkeys) {

		foreach ($fkeys as $key => $reference) {			
			$query_str = "ALTER TABLE `".$table_name."` ";
			$query_str .= "ADD FOREIGN KEY (`".$key."`) ";
			$query_str .= "REFERENCES ".$reference;
			$query = $this->db->prepare($query_str);
			$query->execute();
		}

	}

	private function indices($table_name, $indices) {

		foreach ($indices as $key => $type) {	
			$index_name = str_replace(array(","," "),"",$key);
			$query_str = "ALTER TABLE `".$table_name."` ";
			$query_str .= "ADD ".$type." INDEX ".$index_name." (".$key.") ";
			$query = $this->db->prepare($query_str);
			$query->execute();
		}
	}

	private function insert($table_name, $header, $record) {
		
		$i = 0;

		$query_str = "INSERT INTO `".$table_name."` (";		
		foreach ($header as $column => $type) {
			$query_str .= "`".$column."`";
			if ($i<count($header)-1)
				$query_str .= ",";
			$i++;
		}

		$query_str .= ") VALUES (";

		$i = 0;

		foreach ($record as $key => $elem) {

			$elem = trim($elem);

			switch ($header[$key]) {
				case 'int': $query_str .= $this->strToInt($elem); break;
				case 'date': $query_str .= $this->strToDate($elem); break;
				case 'float': $query_str .= $this->strToFloat($elem); break;
				default: $query_str .= "'".addslashes($elem)."'"; break;
			}

			if ($i<count($record)-1) {
				$query_str .= ",";
			}
			
			$i++;
		}

		$query_str .= ")";

		$query = $this->db->prepare($query_str);
		$query->execute();
		return $this->db->errorCode();
	}

	private function delete($table_name, $key, $value) {
		$query_str = "DELETE FROM $table_name WHERE $key = '".$value."'";
		$query = $this->db->prepare($query_str);
		echo $query_str."\n";
		$query->execute();
	}

	private function replace($table_name, $header, $record) {
		
		$i = 0;

		$query_str = "REPLACE INTO `".$table_name."` (";		
		foreach ($header as $column => $type) {
			$query_str .= "`".$column."`";
			if ($i<count($header)-1)
				$query_str .= ",";
			$i++;
		}

		$query_str .= ") VALUES (";

		$i = 0;

		foreach ($record as $key => $elem) {

			$elem = trim($elem);

			switch ($header[$key]) {
				case 'int': $query_str .= $this->strToInt($elem); break;
				case 'date': $query_str .= $this->strToDate($elem); break;
				case 'float': $query_str .= $this->strToFloat($elem); break;
				default: $query_str .= "'".addslashes($elem)."'"; break;
			}

			if ($i<count($record)-1) {
				$query_str .= ",";
			}
			
			$i++;
		}

		$query_str .= ")";

		$query = $this->db->prepare($query_str);
		echo $query_str."\n";
		$query->execute();
	}

	public function backup() {
		$name = date('d.m.Y-H.i.s');
		mkdir($this->destination."/".$name);
		$content = $this->getFileList();

		foreach ($content as $file) {
			rename($this->destination."/".$file, $this->destination."/".$name."/".$file);
		}

		fwrite($this->log_handle, "\n\n\n\n[".date("d-m-Y H:i:s")."] BACKUP: \t\tOld files has been moved into backup folder\n");

	}

	// download resources
	public function download($source) {
		switch ($source) {
			case 'ellegi2'	: $this->ftp($source); 		break;
			case 'cgross'	: $this->ftp($source); 		break;
			case 'ingram'	: $this->ftp($source); 		break;
			case 'esprinet'	: $this->ftp($source); 		break;
			case 'ingram2'	: $this->ftp($source); 		break;
			case 'mytho' 	: $this->local($source); 	break;
			case 'icecat'	: $this->http($source); 	break;
		}
	}

	// convert files to a common standard (csv) and renames every file to a clearer name
	public function convert() {

		$cycles = 3;

		for ($i = 0; $i < $cycles; $i++) {

			$content = $this->getFileList();

			foreach ($content as $file) {		

				// get file extension
				$temp = explode(".",$file);
				$extension = strtolower($temp[count($temp)-1]);

				// unzip
				if ($extension == 'zip')
					$this->convertZip($file);

				if ($extension == 'gz') 
					$this->convertGzip($file);

				// converts xls or xlst to csv
				if ($extension == 'xls' || $extension == 'xlsx')
					$this->convertXls($file, $extension);

				// replace txt and txt extension to csv extensions
				if ($extension == 'txt')
					$this->convertExtension($file);

				// converts csv (semicolon separated values) to csv (comas separated values)
				if ($extension == 'csv')
					$this->convertCsv($file);

			}
		}

		$this->rename_files();

	}

	public function importFilesToDb($source) {

		$this->dropAndCreate($source);
		$this->update($source);

	}

	public function dropAndCreate($source) {

		foreach ($this->files[$source] as $file) {
			if ($file["update_type"] == "drop_and_create" && ($this->period == "day" || $this->period == $file['period']) ) {
				// drop old table
				$table_name = str_replace(".csv", "", $file['local']);
				$file_name = $this->destination."/".$file['local'];

				// if source file exists and header definition is set then 
				if (file_exists($file_name) && isset($file['header'])) {
					$table_name = str_replace(".csv", "", $file['local']);
					$this->drop($table_name);
				}
				else {
					fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] UNDROPPED: \t$table_name\n");
				}
			}
		}

		foreach ($this->files[$source] as $file) {

			if ($file["update_type"] == "drop_and_create" && ($this->period == "day" || $this->period == $file['period']) ) {

				$table_name = str_replace(".csv", "", $file['local']);
				$file_name = $this->destination."/".$file['local'];

				// if source file exists and header definition is set then 
				if (file_exists($file_name) && isset($file['header'])) {

					$content = $this->csv_to_array($this->destination."/".$file['local'], $file['header']);

					// create table
					$this->create($table_name, $file['header']);

					// add primary keys
					if (isset($file['pkeys']))		
						$this->pkeys($table_name, $file['pkeys']);

					// add foreign keys
					if (isset($file['fkeys']))		
						$this->fkeys($table_name, $file['fkeys']);					

					// add indices
					if (isset($file['indices']))	
						$this->indices($table_name, $file['indices']);

					// insert records
					foreach ($content as $row) {
						$result = $this->insert($table_name, $file['header'], $row);
					}
				}

				else {
					fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] UNTOUCHED: \t$table_name\n");
				}
			}
		}
	}

	public function update($source) {
		foreach ($this->files[$source] as $file) {
			if ($file["update_type"] == "update" && $this->period == "halfhour") {
				if ($file['local'] == "ingram_listino_light.csv") {
					$content = $this->csv_to_array($this->destination."/".$file['local'],$file['header']);
					foreach ($content as $record) {
						switch ($record['action_indicator']) {
							case "A" : $this->insert("ingram_listino",$file['header'],$record);
							case "D" : $this->delete("ingram_listino","ingram_part_number",$record["ingram_part_number"]);
							case "C" : $this->replace("ingram_listino",$file['header'],$record);
						}
					}
				}
			}
		}
	}

	public function scanIcecat() {
		$icecat = $this->csv_to_array($this->destination."/icecat.csv",$this->files['icecat'][0]['header'],"\t");	
		for ($i = 0; $i < count($icecat); $i++) {

			$query = $this->db->prepare("SELECT * FROM products WHERE part_number = '".$icecat[$i]["prod_id"]."'");
			$query->execute();
			
			if ($query->rowCount() == 1) {
				
				$xml = simplexml_load_file("http://".$this->user['icecat'].":".$this->password["icecat"]."@".$this->host['icecat'].'/'.$icecat[$i]["path"]);
				
				if ($xml) {
					$part_number = $xml->Product['Prod_id'];
					$high_pic = $xml->Product['HighPic'];
					$low_pic = $xml->Product['LowPic'];
					$thumb_pic = $xml->Product['ThumbPic'];
					$long_desc = $xml->Product->SummaryDescription->LongSummaryDescription;
					$short_desc = $xml->Product->SummaryDescription->ShortSummaryDescription;
					$query = $this->db->prepare("REPLACE INTO icecat (`part_number`,`high_pic`,`low_pic`,`thumb_pic`,`long_desc`,`short_desc`) VALUES ('".$part_number."','".$high_pic."','".$low_pic."','".$thumb_pic."','".$long_desc."','".$short_desc."')");
					echo "REPLACE INTO icecat (`part_number`,`high_pic`,`low_pic`,`thumb_pic`,`long_desc`,`short_desc`) VALUES ('".$part_number."','".$high_pic."','".$low_pic."','".$thumb_pic."','".$long_desc."','".$short_desc."')\n";
					$query->execute();
				}
			}
		}
	}

	public function writeProductsTable() {

		$this->drop($this->final);

		$query = $this->db->prepare("SET @n := 0;
									CREATE TABLE ".$this->final."
									SELECT @n := @n + 1 as `id`, `name`,`part_number`, `mytho_code`, `original_cat_id`, `original_subcat_id`, 
									`cat_id`, `subcat_id`, `original_manufacturer_id`, `manufacturer_id`, 
									`price` as `price`, `retail_price`, `availability`, 
									`source`,`state`,`promo` 
									FROM 
									(
										(
										SELECT desart as 'name', codfor as 'part_number', codart as 'mytho_code', clin1 as 'original_cat_id', clin2 as 'original_subcat_id',
										clin1 as 'cat_id', clin2 as 'subcat_id', cpro as 'original_manufacturer_id', cpro as 'manufacturer_id',
										preart2 as 'retail_price', preart4 as 'price', disp as 'availability', 'M' as 'source', stato as 'state', promo as 'promo',
										1 as 'priority'
										FROM mytho_listino
										INNER JOIN mytho_produttori_filter ON mytho_listino.cpro = mytho_produttori_filter.mytho_produttore_id
										INNER JOIN mytho_categorie_filter ON (mytho_listino.clin1 = mytho_categorie_filter.mytho_cod1 AND mytho_listino.clin2 = mytho_categorie_filter.mytho_cod2)
										WHERE codfor != ''
										)
										UNION
										(
										SELECT descrizione as 'name', codiceProduttore as 'part_number', NULL as 'mytho_code', fam as 'original_cat_id', catMerc as 'original_subcat_id', 
										mytho_categoria_id as 'cat_id', mytho_sottocategoria_id as 'subcat_id', prod as 'original_manufacturer_id', mytho_produttore_id as 'manufacturer_id',
										prezzoRivenditore as 'retail_price', prezzoListino as 'price', dispo as 'availability', 'ES' as 'source', NULL as 'state', NULL as 'promo',
										0 as 'priority'
										FROM esprinet_listino
										LEFT JOIN esprinet_mytho_produttori_map ON (esprinet_listino.prod = esprinet_mytho_produttori_map.esprinet_produttore_id)
										LEFT JOIN esprinet_mytho_categorie_map ON (esprinet_listino.catMerc = esprinet_mytho_categorie_map.esprinet_categoria_id AND esprinet_listino.fam = esprinet_mytho_categorie_map.esprinet_sottocategoria_id)
          								INNER JOIN esprinet_produttori_filter ON (esprinet_listino.prod = esprinet_produttori_filter.esprinet_produttore_id)
								        INNER JOIN esprinet_categorie_filter ON (esprinet_listino.catMerc = esprinet_categorie_filter.esprinet_categoria_id AND esprinet_listino.fam = esprinet_categorie_filter.esprinet_sottocategoria_id)

										# da tenere separate dalla query sopra nel caso avremo piú fornitori in futuro (gestire tramite UNIONs)
										LEFT JOIN mytho_categorie ON (esprinet_mytho_categorie_map.mytho_categoria_id = mytho_categorie.cod1 AND esprinet_mytho_categorie_map.mytho_sottocategoria_id = mytho_categorie.cod2)
										LEFT JOIN mytho_produttori ON (esprinet_mytho_produttori_map.mytho_produttore_id = mytho_produttori.cod)
										GROUP BY part_number
										ORDER BY price, availability DESC
										)

									) d2
									WHERE availability > 0
									GROUP BY part_number
									ORDER BY priority DESC");

			$query->execute();

			$query = $this->db->prepare("ALTER TABLE products ADD FULLTEXT INDEX (`name`,`part_number`)");
			$query->execute();

			fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] CREATE: \t\t".$this->final." TABLE\n");
	}
	
	public function writeLog($msg) {
		fwrite($this->log_handle, "[".date("d-m-Y H:i:s")."] ".$msg."\n");
	}
}

?>