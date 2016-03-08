<?php 

	class flatter {

		const DEBUG = false;
		const TECNOCOMPUTER = "products_tecnocomputer_flat";
		const CZESTERNI = "products_czesterni_flat";
		const GRIZZLY = "products_grizzly_flat";
		private static $tecnocomputer_locks = array(self::TECNOCOMPUTER);
		private static $tecnocomputer_needs = array('mytho_listino','mytho_listino_completo','esprinet_listino','esprinet_disponibilita','esprinet_arrivi','esprinet_mytho_produttori_map','esprinet_mytho_categorie_map','cgross_listino','cgross_mytho_categorie_map','cgross_mytho_produttori_map','ingram_listino','ingram_disponibilita','ingram_mytho_categorie_map','ingram_mytho_produttori_map','ingram_siae','focelda_listino','focelda_mytho_produttori_map','focelda_mytho_categorie_map');
		private static $czesterni_locks = array(self::CZESTERNI);
		private static $czesterni_needs = array('cgross_listino','cgross_mytho_categorie_map','cgross_mytho_produttori_map');
		private static $grizzly_locks = array(self::GRIZZLY);
		private static $grizzly_needs = array('grizzly_listino');

		function __construct($args = null) 
		{
			$this->args = $args;
			$this->db = helper::getPDOHandle(true);
			if (self::DEBUG) self::debugBlock();
		}

		public function actionCreate()
		{
			$target = $this->args[0];

			$createTable = "createProducts".ucfirst($target)."FlatTable";
			$fillTable = "updateProducts".ucfirst($target)."FlatTable";

			if (method_exists($this,$createTable) && method_exists($this,$fillTable)) {
				$now = date("Y-m-d H:i:s");
				$proceed = $this->checkTablesBlocks($target);
				if ($proceed) {
					$proceed = $this->checkEmptyTables($target);
					if ($proceed) {
						$this->$createTable();
						$this->$fillTable($now);
					}
				}
				else {
					$table_list = implode(",",self::${$target.'_needs'});
					helper::writeLine("The content of some important tables (".$table_list.") is not present.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
					helper::writeLine("Make sure that all necessary tables for creating the flat got correctly imported and try again.",helper::ERROR_ALERT,helper::DIE_AFTER_MSG,helper::LOG_MSG);
				}
			}
			else {
				helper::writeLine("The creation table method table or the filler table method do not exists.",helper::ERROR_ALERT,helper::LOG_MSG);
			}
		}
		
		private function checkEmptyTables($target)
		{
			if (isset(self::${$target."_needs"})) {
				foreach (self::${$target."_needs"} as $needs) {
					$query = $this->db->query("SELECT COUNT(*) as cnt FROM $needs");
					$resultTest = $query->fetchObject();
					if (!$resultTest || !$resultTest->cnt) {
						helper::writeLine("No rows for table $needs", helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);
						return false;
					}
				}
			}
			return true;
		}

		private function checkTablesBlocks($target) 
		{
			$result = false;
					
			if (isset(self::${$target."_locks"})) {

				$result = true;
				$tables_to_check = array_merge(self::${$target."_locks"}, isset(self::${$target."_needs"}) ? self::${$target."_needs"} : array());

				foreach ($tables_to_check as $table_to_check) {
					$query_str = $this->db->prepare("SELECT * FROM system_resources WHERE local_name = '".$table_to_check."'");
					$query_str->execute();
					$record = $query_str->fetch();

					if (!empty($record)) {
						if ($record['locked'] == '0') {
							helper::writeLine($table_to_check." has passed the lock check",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
						}
						else {
							$result = false;
							helper::writeLine($table_to_check." is locked",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
						}
					}
				}
			}
			else {
				helper::writeLine("the private static property $".$target."_locks doesn't exists.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
				helper::writeLine("Make sure to decleare one (even an empty array) in the flatter class",helper::ERROR_ALERT,helper::DIE_AFTER_MSG,helper::LOG_MSG);
			}

			return $result;
		}

		private function lockTables($target)
		{
			$tables_to_check = is_array($target) ? $target : self::${$target."_locks"};

			foreach ($tables_to_check as &$table_to_check) {
				$table_to_check = $table_to_check." WRITE";
			}

			$query = $this->db->prepare("LOCK TABLES ".implode(",",$tables_to_check).";");
			$query->execute();
		}

		private function unlockTables() {

			$query = $this->db->prepare("UNLOCK TABLES");
			$query->execute();
		}
		
		private function getTecnocomputerFlatQuery($limitSources = array(), $map = null)
		{
			$sql = "SELECT *
					FROM (
						SELECT CONCAT_WS('|||',manufacturer_id,part_number) AS `id`, `name`, `EAN`, `part_number`,  GROUP_CONCAT(`mytho_code`) AS `mytho_code`, `original_cat_id`, `original_subcat_id`, 
								`cat_id`, `subcat_id`, `original_manufacturer_id`, `manufacturer_id`, 
								`enduser_price`, `internal_availability`, `external_availability`, `in_arrival`, `arrival_date`,
								`width`,`height`,`depth`,`weight`, `min_orderable_qty`, `source`,`state`,`promo`, `retailers_price`, `list_price`
						FROM (
							SELECT * 
							FROM (
									/*@SOURCE=MYTHO*/
									(
										SELECT *
										FROM (
												SELECT desart AS 'name', EAN AS 'EAN', codfor AS 'part_number', codart AS 'mytho_code', clin1 AS 'original_cat_id', clin2 AS 'original_subcat_id',
													   clin1 AS 'cat_id', clin2 AS 'subcat_id', cpro AS 'original_manufacturer_id', cpro AS 'manufacturer_id', PREART3 AS 'enduser_price',
													   disp AS 'internal_availability', 0 AS 'external_availability', OFOR AS 'in_arrival', DTFOR AS 'arrival_date', 'M' AS 'source', stato AS 'state', promo AS 'promo',
													   NULL AS 'width', NULL AS 'height', NULL AS 'depth', peso AS 'weight', NULL AS 'min_orderable_qty', 2 AS 'priority',
													   PREART2 AS 'retailers_price', PREART4 AS 'list_price'
												FROM mytho_listino
												WHERE codfor != ''
												ORDER BY 1 - CASE WHEN disp > 0 THEN 0 ELSE 1 END DESC, preart3, disp DESC
										) m
										GROUP BY part_number
									)
									/*@END-SOURCE=MYTHO*/
									/*@SOURCE=CGROSS (sospeso)
									(
										SELECT * FROM (
												SELECT descrizione AS 'name', '' AS 'EAN', codice_articolo_fornitore AS 'part_number', codart AS 'mytho_code', codice_macrocategoria AS 'original_cat_id', CONCAT_WS('$',codice_categoria,codice_sottocategoria) AS 'original_subcat_id', 
													   mytho_categoria_id AS 'cat_id', mytho_sottocategoria_id AS 'subcat_id', produttore AS 'original_manufacturer_id', mytho_produttore_id AS 'manufacturer_id',
													   prezzo_netto+`contr.siae`+`contr.raee` AS 'enduser_price', 0 AS 'internal_availability', immediata AS 'external_availability', futura-immediata AS 'in_arrival',
													   '' AS 'arrival_date', 'CG' AS 'source', NULL AS 'state', NULL AS 'promo',
													   0 AS 'width', 0 AS 'height', 0 AS 'depth', 0 AS 'weight', 1 AS 'min_orderable_qty', 0 AS 'priority',
													   prezzo_netto+`contr.siae`+`contr.raee` AS 'retailers_price', prezzo_listino AS 'list_price'
												FROM cgross_listino
												INNER JOIN cgross_mytho_produttori_map ON (cgross_listino.produttore = cgross_mytho_produttori_map.cgross_produttore_id)
												INNER JOIN cgross_mytho_categorie_map ON (cgross_listino.codice_macrocategoria = cgross_mytho_categorie_map.cgross_categoria_id AND CONCAT_WS('$',cgross_listino.codice_categoria,cgross_listino.codice_sottocategoria) = cgross_mytho_categorie_map.cgross_sottocategoria_id)
												LEFT JOIN mytho_listino_completo ON codfor=cgross_listino.codice_articolo_fornitore  AND codfor<>'' AND codart NOT LIKE 'H%' AND codart NOT LIKE 'R%'
												ORDER BY 1 - CASE WHEN cgross_listino.immediata > 0 THEN 0 ELSE 1 END DESC, prezzo_netto DESC
										) e
										GROUP BY part_number
									)
									@END-SOURCE=CGROSS*/
									/*@SOURCE=FOCELDA*/
									UNION(
										SELECT * FROM (
												SELECT Descr AS 'name', EAN , Cod AS 'part_number', codart AS 'mytho_code', Linea AS 'original_cat_id', Famiglia AS 'original_subcat_id', 
													   mytho_categoria_id AS 'cat_id', mytho_sottocategoria_id AS 'subcat_id', Marca AS 'original_manufacturer_id', mytho_produttore_id AS 'manufacturer_id',
													   Prezzo AS 'enduser_price', 0 AS 'internal_availability', Disponibili AS 'external_availability', 0 AS 'in_arrival',
													   '' AS 'arrival_date', 'FO' AS 'source', NULL AS 'state', NULL AS 'promo',
													   NULL AS 'width', NULL AS 'height', NULL AS 'depth', Peso AS 'weight', 0 AS 'min_orderable_qty', 0 AS 'priority',
													   Prezzo AS 'retailers_price', Prezzo AS 'list_price'
												FROM focelda_listino
												INNER JOIN focelda_mytho_produttori_map ON (focelda_listino.Marca = focelda_mytho_produttori_map.focelda_produttore_id)
												INNER JOIN focelda_mytho_categorie_map ON (focelda_listino.Linea = focelda_mytho_categorie_map.focelda_categoria_id AND focelda_listino.Famiglia = focelda_mytho_categorie_map.focelda_sottocategoria_id)
												LEFT JOIN mytho_listino_completo ON codfor=focelda_listino.Cod  AND codfor<>'' AND codart NOT LIKE 'H%' AND codart NOT LIKE 'R%'
												ORDER BY 1 - CASE WHEN Disponibili > 0 THEN 0 ELSE 1 END DESC, Prezzo DESC
										) e
										GROUP BY part_number
									)
									/*@END-SOURCE=FOCELDA*/
									/*@SOURCE=ESPRINET*/
									UNION(
										SELECT * FROM (
												SELECT descrizione AS 'name', codiceEAN AS 'EAN', codiceProduttore AS 'part_number', codart AS 'mytho_code', catMerc AS 'original_cat_id', fam AS 'original_subcat_id', 
													   mytho_categoria_id AS 'cat_id', mytho_sottocategoria_id AS 'subcat_id', prod AS 'original_manufacturer_id', mytho_produttore_id AS 'manufacturer_id',
													   prezzoRivenditore+raee AS 'enduser_price', 0 AS 'internal_availability', dispo AS 'external_availability', esprinet_listino.arrivi AS 'in_arrival',
													   esprinet_arrivi.arrival_date AS 'arrival_date', 'ES' AS 'source', NULL AS 'state', NULL AS 'promo',
													   lunghezza AS 'width', altezza AS 'height', profondita AS 'depth', pesoLordo AS 'weight', qtaMinimaOrd AS 'min_orderable_qty', 0 AS 'priority',
													   prezzoRivenditore+raee AS 'retailers_price', prezzolistino AS 'list_price'
												FROM esprinet_listino
												LEFT JOIN esprinet_arrivi ON (esprinet_arrivi.codice = esprinet_listino.codice)
												INNER JOIN esprinet_mytho_produttori_map ON (esprinet_listino.prod = esprinet_mytho_produttori_map.esprinet_produttore_id)
												INNER JOIN esprinet_mytho_categorie_map ON (esprinet_listino.catMerc = esprinet_mytho_categorie_map.esprinet_categoria_id AND esprinet_listino.fam = esprinet_mytho_categorie_map.esprinet_sottocategoria_id)
												LEFT JOIN mytho_listino_completo ON codfor=esprinet_listino.codiceproduttore  AND codfor<>'' AND codart NOT LIKE 'H%' AND codart NOT LIKE 'R%'
												WHERE tipo != 'N' 
													  AND (dispo>0 OR esprinet_listino.arrivi>0 OR esprinet_listino.stato<>'C')
												ORDER BY 1 - CASE WHEN dispo > 0 THEN 0 ELSE 1 END DESC, prezzoRivenditore DESC
										) e
										GROUP BY part_number
									)
									/*@END-SOURCE=ESPRINET*/
									/*@SOURCE=INGRAM*/
									UNION(
										SELECT * FROM (
												SELECT ingram_part_description_1 AS 'name', IF(upc_code<>'9999999999999', upc_code, '') AS 'EAN', vendor_part_number AS 'part_number', codart AS 'mytho_code', SUBSTRING(ingram_cat_subcat, 1, 2) AS 'original_cat_id', SUBSTRING(ingram_cat_subcat, 3, 2) AS 'original_subcat_id', 
													   mytho_categoria_id AS 'cat_id', mytho_sottocategoria_id AS 'subcat_id', ingram_listino.vendor_number AS 'original_manufacturer_id', mytho_produttore_id AS 'manufacturer_id',
													   custom_price + IFNULL(ingram_siae.siae,0) AS 'enduser_price', 0 AS 'internal_availability', availability AS 'external_availability', ordered AS 'in_arrival',
													   arrival_date AS 'arrival_date', 'IN' AS 'source', NULL AS 'state', NULL AS 'promo',
													   width AS 'width', height AS 'height', `length` AS 'depth', weight AS 'weight', 1 AS 'min_orderable_qty', 0 AS 'priority',
													   custom_price + IFNULL(ingram_siae.siae,0) AS 'retailers_price', retail_price AS 'list_price'
												FROM ingram_listino
												INNER JOIN ingram_disponibilita ON ingram_disponibilita.ingram_part_number=ingram_listino.ingram_part_number
												INNER JOIN ingram_mytho_produttori_map ON (ingram_listino.vendor_number = ingram_mytho_produttori_map.ingram_produttore_id)
												INNER JOIN ingram_mytho_categorie_map ON (ingram_listino.ingram_cat_subcat = CONCAT(ingram_mytho_categorie_map.ingram_categoria_id, ingram_mytho_categorie_map.ingram_sottocategoria_id))
												LEFT JOIN ingram_siae ON ingram_listino.ingram_part_number=ingram_siae.ingram_part_number AND country='IT'
												LEFT JOIN mytho_listino_completo ON codfor=ingram_listino.vendor_part_number  AND codfor<>'' AND codart NOT LIKE 'H%' AND codart NOT LIKE 'R%'
												ORDER BY 1 - CASE WHEN ingram_disponibilita.availability > 0 THEN 0 ELSE 1 END DESC, custom_price DESC
										) e
										GROUP BY part_number
									)
									/*@END-SOURCE=INGRAM*/
									/* UNION (for future sources)
									(		
										SELECT *
										FROM (
											SELECT ...
											FROM ...
											ORDER BY 1 - CASE WHEN ... > 0 THEN 0 ELSE 1 END DESC, ..., ..., ... DESC
										) e2
										GROUP BY part_number
									)*/
							) d1
							/*Ordina per priorità, il resto dell'order by serve per decidere quali record prendere tra due fonti che hanno la stessa
							priorità, in questo caso vince la fonte che ha disponibilità o, nel caso la abbiano entrambe, quella con prezzo minore*/
							ORDER BY priority DESC, 1 - CASE WHEN external_availability > 0 THEN 0 ELSE 1 END DESC, enduser_price DESC
						) d2 
						GROUP BY part_number
						ORDER BY priority DESC, 1 - CASE WHEN external_availability > 0 THEN 0 ELSE 1 END DESC, enduser_price DESC
					)d3
					/*Raggruppamento finale dei prodotti per EAN, ma solo nel caso sia presente*/
					GROUP BY IF(EAN IS NOT NULL AND EAN<>'', EAN, UUID())";
			
			//Remove unwanted sources
			if (count($limitSources)) {
				$lastIndex = 0;
				while (preg_match("#\s*/\*@SOURCE=(\w+)\*/#si", $sql, $match, PREG_OFFSET_CAPTURE, $lastIndex)) {
					$sourceName = $match[1][0];
					$startMatchIndex = $match[0][1];
					$endFirstMatchIndex = $startMatchIndex + strlen($match[0][0]);
					if (!in_array($sourceName, $limitSources)) {
						if (preg_match("#\s*/\*@END-SOURCE=" . preg_quote($sourceName, "#") . "\*/(?:\s*UNION)?#si", $sql, $match, PREG_OFFSET_CAPTURE, $endFirstMatchIndex)) {
							$sql = substr_replace($sql, "", $startMatchIndex, $match[0][1] + strlen($match[0][0]) - $startMatchIndex);
						} else {
							throw new Exception("End of source $sourceName not found");
						}
					} else {
						$lastIndex = $endFirstMatchIndex;
					}
				}
				
				//Remove uncorrect unions
				$sql = preg_replace("#UNION\s*([^\s\(])#i", "$1", $sql);
			}
			
			//Replace the initial * with required fields
			if ($map !== null) {
				$prefix = isset($map["prefix"]) ? $map["prefix"] : "";
				$sqlFields = array();
				foreach ($map["fields"] as $field) {
					$sField = "`$field`";
					if ($prefix) {
						$sField .= " as `$prefix$field`";
					}
					$sqlFields[] = $sField;
				}
				$sql = preg_replace("#\*#", implode(",", $sqlFields), $sql, 1);
			}
			
			return $sql;
		}

		/**
		* Add table products_tecnocomputer to e_sync schema where all 
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createProductsTecnocomputerFlatTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db, self::TECNOCOMPUTER)) {

				helper::writeLine("Started creating ".self::TECNOCOMPUTER,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

 				$query = $this->db->prepare("CREATE TABLE `".self::TECNOCOMPUTER."` (
											  `id` varchar(138) CHARACTER SET utf8 NOT NULL DEFAULT '',
											  `name` mediumtext CHARACTER SET utf8,
											  `EAN` varchar(15) CHARACTER SET utf8 DEFAULT NULL,
											  `part_number` varchar(35) CHARACTER SET utf8 DEFAULT NULL,
											  `mytho_code` text CHARACTER SET utf8,
											  `original_cat_id` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
											  `original_subcat_id` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
											  `cat_id` varchar(10) CHARACTER SET utf8 DEFAULT NULL,
											  `subcat_id` varchar(10) CHARACTER SET utf8 DEFAULT NULL,
											  `original_manufacturer_id` varchar(3) CHARACTER SET utf8 DEFAULT NULL,
											  `manufacturer_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,
											  `enduser_price` float DEFAULT NULL,
											  `internal_availability` decimal(32,0) DEFAULT NULL,
											  `external_availability` decimal(32,0) DEFAULT NULL,
											  `in_arrival` int(11) DEFAULT NULL,
											  `arrival_date` date DEFAULT NULL,
											  `width` float DEFAULT NULL,
											  `height` float DEFAULT NULL,
											  `depth` float DEFAULT NULL,
											  `weight` float DEFAULT NULL,
											  `min_orderable_qty` int(11) DEFAULT NULL,
											  `source` varchar(2) CHARACTER SET utf8 NOT NULL DEFAULT '',
											  `state` varchar(1) CHARACTER SET utf8 DEFAULT NULL,
											  `promo` varchar(1) CHARACTER SET utf8 DEFAULT NULL,
											  `retailers_price` float DEFAULT NULL,
											  `list_price` float DEFAULT NULL,
											  `updated` datetime DEFAULT NULL,
											  PRIMARY KEY (`id`)
											) ENGINE=MyISAM DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					helper::writeLine("Finished creating ".self::TECNOCOMPUTER,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
				} else {
					helper::writeLine("Something went wrong creating ".self::TECNOCOMPUTER,helper::ERROR_ALERT,helper::DIE_AFTER_MSG,helper::LOG_MSG);
				}

			}

			return $result;
		}

		private function updateProductsTecnocomputerFlatTable($now) 
		{
			helper::writeLine("Started updating " . self::TECNOCOMPUTER, helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);

			$updated = $deleted = $untouched = 0;
			$tracker = array();

			// check records before sync 
			$query = $this->db->prepare("SELECT id FROM " . self::TECNOCOMPUTER);
			$query->execute();
			$temp = $query->fetchAll();

			foreach ($temp as $key => $record) {
				$tracker[$record['id']] = 0;
			}

			$query = $this->db->prepare($this->getTecnocomputerFlatQuery());

			$query->execute();
			
			$result = $query->fetchAll(PDO::FETCH_ASSOC);
			
			//Remove £
			$remChar = utf8_encode(chr(163));
			$remCharLen = strlen($remChar);

			$this->lockTables(array(self::TECNOCOMPUTER));
			
			// sync 
			foreach ($result as $row) {

				$sets = $columns = $values = array();

				foreach ($row as $key => $value) {
					if ($key === "name" && $value && substr($value, 0, $remCharLen) === $remChar) {
						$value = substr($value, $remCharLen);
					}
					$sets[] = $key." = '".addslashes($value)."'";
					$columns[] = "`".$key."`";
					$values[] = "'".addslashes($value)."'";
				}

				$sets = implode(",",$sets);
				$columns = implode(",", $columns);
				$values = implode(",", $values);

				$query_str = "INSERT INTO ".self::TECNOCOMPUTER." ($columns, updated) VALUES ($values, '".$now."') ON DUPLICATE KEY UPDATE $sets";
				$query = $this->db->prepare($query_str);
				@$query->execute();
				
				if ($query->rowCount()) {
					$query_str = "UPDATE ".self::TECNOCOMPUTER." SET updated = '".$now."' WHERE id = '".$row['id']."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$updated++;
				}
				else {
					$untouched++;
				}

				$tracker[$row['id']] = 1;
			}

			// remove not existing records

			foreach ($tracker as $key => $status) {
				if (!$status) {
					$query_str = "DELETE FROM ".self::TECNOCOMPUTER." WHERE id = '".$key."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$deleted++;
				}
			}
			
			$this->unlockTables();

			helper::writeLine("Results for updating ".self::TECNOCOMPUTER.":",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("INSERTED/UPDATED RECORDS: ".$updated,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("UNTOUCHED RECORDS: ".$untouched,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("DELETED RECORDS: ".$deleted,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

		}
		
		private function createProductsCzesterniFlatTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db, self::CZESTERNI)) {

				helper::writeLine("Started creating ".self::CZESTERNI,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

 				$query = $this->db->prepare("CREATE TABLE `".self::CZESTERNI."` (
											  `b2b_id` varchar(138) CHARACTER SET utf8 NOT NULL DEFAULT '',
											  `b2b_external_availability` decimal(32,0) DEFAULT NULL,
											  `b2b_in_arrival` int(11) DEFAULT NULL,
											  `b2b_arrival_date` date DEFAULT NULL,
											  `b2b_min_orderable_qty` int(11) DEFAULT NULL,
											  `b2b_source` varchar(2) CHARACTER SET utf8 NOT NULL DEFAULT '',
											  `b2b_retailers_price` float DEFAULT NULL,
											  `b2b_updated` datetime DEFAULT NULL,
											  PRIMARY KEY (`b2b_id`)
											) ENGINE=MyISAM DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					helper::writeLine("Finished creating " . self::CZESTERNI, helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);
				} else {
					helper::writeLine("Something went wrong creating " . self::CZESTERNI, helper::ERROR_ALERT, helper::DIE_AFTER_MSG, helper::LOG_MSG);
				}

			}

			return $result;
		}

		private function updateProductsCzesterniFlatTable($now) 
		{
			helper::writeLine("Started updating " . self::CZESTERNI, helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);

			$updated = $deleted = $untouched = 0;
			$tracker = array();

			// check records before sync 
			$query = $this->db->prepare("SELECT b2b_id FROM " . self::CZESTERNI);
			$query->execute();
			$temp = $query->fetchAll();

			foreach ($temp as $key => $record) {
				$tracker[$record['b2b_id']] = 0;
			}

			$query = $this->db->prepare(
				$this->getTecnocomputerFlatQuery(
					array("INGRAM"),
					array(
						"prefix" => "b2b_",
						"fields" => array("id", "external_availability", "in_arrival", "arrival_date", "min_orderable_qty", "source", "retailers_price")
					)
				)
			);

			$query->execute();
			
			$result = $query->fetchAll(PDO::FETCH_ASSOC);

			$this->lockTables(array(self::CZESTERNI));
			
			// sync 
			foreach ($result as $row) {

				$sets = $columns = $values = array();

				foreach ($row as $key => $value) {
					$sets[] = $key." = '".addslashes($value)."'";
					$columns[] = "`".$key."`";
					$values[] = "'".addslashes($value)."'";
				}

				$sets = implode(",",$sets);
				$columns = implode(",", $columns);
				$values = implode(",", $values);

				$query_str = "INSERT INTO ".self::CZESTERNI." ($columns, b2b_updated) VALUES ($values, '".$now."') ON DUPLICATE KEY UPDATE $sets";
				$query = $this->db->prepare($query_str);
				@$query->execute();
				
				if ($query->rowCount()) {
					$query_str = "UPDATE ".self::CZESTERNI." SET b2b_updated = '".$now."' WHERE b2b_id = '".$row['b2b_id']."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$updated++;
				}
				else {
					$untouched++;
				}

				$tracker[$row['b2b_id']] = 1;
			}

			// remove not existing records

			foreach ($tracker as $key => $status) {
				if (!$status) {
					$query_str = "DELETE FROM ".self::CZESTERNI." WHERE b2b_id = '".$key."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$deleted++;
				}
			}
			
			$this->unlockTables();

			helper::writeLine("Results for updating ".self::CZESTERNI.":",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("INSERTED/UPDATED RECORDS: ".$updated,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("UNTOUCHED RECORDS: ".$untouched,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("DELETED RECORDS: ".$deleted,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

		}
		
		private function createProductsGrizzlyFlatTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db, self::GRIZZLY)) {

				helper::writeLine("Started creating ".self::GRIZZLY,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

 				$query = $this->db->prepare("CREATE TABLE `".self::GRIZZLY."` (
												`id` varchar(50) NOT NULL,
												`CODART` varchar(50) NOT NULL,
												`FAM` varchar(50) DEFAULT NULL,
												`SOTTOFAM` varchar(50) DEFAULT NULL,
												`GENERE` varchar(50) DEFAULT NULL,
												`TIPO_PROD` varchar(50) DEFAULT NULL,
												`STAGIONE` varchar(50) DEFAULT NULL,
												`DESCRIZIONE` varchar(250) DEFAULT NULL,
												`GIACENZA` int(11) DEFAULT NULL,
												`PREZZO` float(12,2) DEFAULT NULL,
												`CODFOR` varchar(50) DEFAULT NULL,
												`TAGLIA` int(11) DEFAULT NULL,
												`COLORE` varchar(50) DEFAULT NULL,
												`updated` datetime DEFAULT NULL,
												PRIMARY KEY (`id`)
											) ENGINE=MyISAM DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					helper::writeLine("Finished creating " . self::GRIZZLY, helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);
				} else {
					helper::writeLine("Something went wrong creating " . self::GRIZZLY, helper::ERROR_ALERT, helper::DIE_AFTER_MSG, helper::LOG_MSG);
				}

			}

			return $result;
		}

		private function updateProductsGrizzlyFlatTable($now) 
		{
			helper::writeLine("Started updating " . self::GRIZZLY, helper::SYSTEM_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);

			$updated = $deleted = $untouched = 0;
			$tracker = array();

			// check records before sync 
			$query = $this->db->prepare("SELECT id FROM " . self::GRIZZLY);
			$query->execute();
			$temp = $query->fetchAll();

			foreach ($temp as $key => $record) {
				$tracker[$record['id']] = 0;
			}

			$query = $this->db->prepare("
				SELECT	CODART, FAM, SOTTOFAM, GENERE, TIPO_PROD, STAGIONE, DESCRIZIONE, GIACENZA, PREZZO,
						CASE
							WHEN FAM='526' THEN REPLACE(CODFOR, 'PU', '')
							WHEN FAM='501' THEN REPLACE(CODFOR, 'NK', '')
							ELSE CODFOR END
						AS CODFOR,
						TAGLIA, COLORE, CODART AS id
				FROM `grizzly_listino`
				WHERE FAM IN ('598', '528', '526', '501') AND SOTTOFAM IN ('006', '001')
			");

			$query->execute();
			
			$result = $query->fetchAll(PDO::FETCH_ASSOC);

			$this->lockTables(array(self::GRIZZLY));
			
			// sync 
			foreach ($result as $row) {

				$sets = $columns = $values = array();

				foreach ($row as $key => $value) {
					$sets[] = $key." = '".addslashes($value)."'";
					$columns[] = "`".$key."`";
					$values[] = "'".addslashes($value)."'";
				}

				$sets = implode(",",$sets);
				$columns = implode(",", $columns);
				$values = implode(",", $values);

				$query_str = "INSERT INTO ".self::GRIZZLY." ($columns, updated) VALUES ($values, '".$now."') ON DUPLICATE KEY UPDATE $sets";
				$query = $this->db->prepare($query_str);
				@$query->execute();
				
				if ($query->rowCount()) {
					$query_str = "UPDATE ".self::GRIZZLY." SET updated = '".$now."' WHERE id = '".$row['id']."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$updated++;
				}
				else {
					$untouched++;
				}

				$tracker[$row['id']] = 1;
			}

			// remove not existing records

			foreach ($tracker as $key => $status) {
				if (!$status) {
					$query_str = "DELETE FROM ".self::GRIZZLY." WHERE id = '".$key."'";
					$query = $this->db->prepare($query_str);
					$query->execute();
					$deleted++;
				}
			}
			
			$this->unlockTables();

			helper::writeLine("Results for updating ".self::GRIZZLY.":",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("INSERTED/UPDATED RECORDS: ".$updated,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("UNTOUCHED RECORDS: ".$untouched,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
			helper::writeLine("DELETED RECORDS: ".$deleted,helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);

		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".FLATTER_ROOT." create [source_name]\t\tCreates flat products table for [source_name]");
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
			die();
		}
	}

?>