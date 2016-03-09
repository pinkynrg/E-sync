<?php 

	class importer {

		const DEBUG = false;

		/**
		* Importer Class Constructor
		* @param 	Arguments 	$args 	It will contain all the arguments passed by the user without the first (Controller) and the second one (Action)
		* @return 	void
		*/
		function __construct($args = null) 
		{
			$this->args = $args;
			$this->db = helper::getPDOHandle();
			if (self::DEBUG) self::debugBlock();
		}

		/**
		* Import all active resource into db
		* @param 	void
		* @return 	void
		*/
		public function actionImport()
		{
			if (isset($this->args[0])) {
				$resource_name = $this->args[0];
				// for auto import
				if ($resource_name == "auto") {
					$current_time = date("H:i");
					$resources = helper::getResources($this->db);
					foreach ($resources as $resource) {
						if (self::hasToBeImported($resource,$current_time)) {
							helper::writeLine("The resource ".$resource['local_name']." started importing",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							$result = self::importResource($resource);
							if ($result) {
								helper::writeLine("The resource ".$resource['local_name']." has been imported correctly",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							}
							else {
								helper::writeLine("The resource ".$resource['local_name']." coudn't be imported",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							}
						}
					}
				}
				// to force import all resources
				elseif ($resource_name == "all") {
					$resources = helper::getResources($this->db);
					foreach ($resources as $resource) {
						helper::writeLine("The resource ".$resource['local_name']." started importing",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						$result = self::importResource($resource);
						if ($result) {
							helper::writeLine("The resource ".$resource['local_name']." has been imported correctly",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						}
						else {
							helper::writeLine("The resource ".$resource['local_name']." coudn't be imported",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
						}
					}
				}
				// to import one resource
				else {
					$resources = helper::getResources($this->db,$resource_name);
					if (!empty($resources)) {
						foreach ($resources as $resource) {
							helper::writeLine("The resource ".$resource['local_name']." started importing",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							$result = self::importResource($resource);
							if ($result) {
								helper::writeLine("The resource ".$resource['local_name']." has been imported correctly",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							}
							else {
								helper::writeLine("The resource ".$resource['local_name']." coudn't be imported",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
							}
						}
					}
					else {
						helper::writeLine("Something went wrong: ".$resource_name." could be locked by another importation or it may not exists.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG,helper::LOG_MSG);
					}
				}
			}
			else {
				self::help();
			}
		}

		/**
		* import a resource into the db
		* @param 	Resource 	$resource 	the resource to be imported
		* @return 	Success 	$success 	true if success, false otherwise
		*/
		private function importResource($resource)
		{	
			$success = false;
			
			self::lockResource($resource['local_name']);
			
			helper::writeLine("Started downloading ".$resource['local_name'],helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
			$local_path = downloader::downloadFile($resource);
			if ($local_path) {
				helper::writeLine("Started converting ".$resource['local_name'],helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
				$local_path = converter::convertFile($resource);
				if ($local_path) {	
					helper::writeLine("Creating mysql table for ".$resource['local_name'],helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);		
					$created = self::createTableFromResource($resource);
					if ($created) {
						helper::writeLine("Filling mysql table for ".$resource['local_name'],helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);		
						$filled = self::fillTableFromResource($resource,$local_path);
					}
				}
			}

			self::unlockResource($resource['local_name']);

			$success = $local_path && $created && $filled;

			return $success;
		}

		/**
		* Create the table for the resource to be imported
		* @param 	Resource 	$resource 	the resource for which the table has to be created
		* @return 	Created 	$created 	true if success, false otherwise
		*/
		private function createTableFromResource($resource)
		{
			$created = false;
			$columns = json_decode($resource['columns'],true,JSON_UNESCAPED_UNICODE);
			$indices = json_decode($resource['indices'],true,JSON_UNESCAPED_UNICODE);

			$query = $this->db->prepare("DROP TABLE IF EXISTS `".$resource['local_name']."`;");
			$deleted = $query->execute();

			$query_str = "CREATE TABLE `".$resource['local_name']."` (";
			
			for ($i=0; $i < count($columns['name']); $i++) {
				$column_type = helper::getRecordById($this->db,'system_valid_column_types',$columns['type'][$i]);
				$query_str .= "`".$columns['name'][$i]."` ".$column_type['dimension'];
				$query_str .= in_array($columns['name'][$i], $indices) ? " NOT NULL" : "";
				$query_str .= ",";
			}

  			if (!empty($indices)) {
  				
  				foreach ($indices as &$index) {
  					$index = "`".$index."`";
					$query_str .= "KEY `" . preg_replace("#\W+#", "", $index) . "_idx` (" . $index . "),";
  				}

  				$index_keys = implode(",",$indices);
  				$query_str .= "UNIQUE KEY `" . $resource['local_name'] . "_uniqueness` (".$index_keys.")";
  			}
			
			$query_str = trim($query_str, ",");
  			$query_str .= ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
	
			$query = $this->db->prepare($query_str);

			$created = $query->execute();

			return $created;
		}

		/**
		* Fill the table of the resource to be imported
		* @param 	Resource 	$resource 	the resource for which the table has to be filled
		* @return 	Created 	$created 	true if success, false otherwise
		*/
		private function fillTableFromResource($resource,$local_path)
		{
			$filled = false;
			$missed = 0;
			$completed = 0;

			if ($local_path) {

				$columns = json_decode($resource['columns'],true);
				$has_header = $resource['csv_header_row'] == 1 ? true : false;

				helper::writeLine("CSV to ARRAY conversion for ".$resource['local_name'],helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);		
				$content = self::csvToArray($local_path,$has_header,$resource['delimiter'],$resource['enclosure']);
				helper::writeLine("Ended CSV to ARRAY conversion for ".$resource['local_name'],helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG);		

				for($i=0; $i<count($columns['type']); $i++) {
					$column_type_record = helper::getRecordById($this->db,'system_valid_column_types',$columns['type'][$i]);
					$column_type[$i] = $column_type_record['name'];
				}

				for ($i=0; $i<count($columns['name']); $i++) {
					$columns['name'][$i] = "`".addslashes($columns['name'][$i])."`";
				}
				
				foreach ($content as $row) {
					
					$query_str = "INSERT INTO `".$resource['local_name']."` (";					
					$query_str .= implode(",",$columns['name']);
					$query_str .= ") VALUES (";

					for ($i=0; $i<count($row); $i++) {
						
						$row[$i] = trim($row[$i]);
						
						switch ($column_type[$i]) {
							case "String": break;
							case "Integer": $row[$i] = intval($row[$i]); break;
							case "Float": $row[$i] = self::stringToMySqlFloat($row[$i]); break;
							case "Date": $row[$i] = self::stringToMySqlDate($row[$i]); break;
							case "Description": break;
						}

						$row[$i] = "'".addslashes($row[$i])."'";
					}

					$query_str .= implode(",",$row);
					$query_str .= ")";

					$query = $this->db->prepare($query_str);
					
					// $start = helper::microtime_float();

					if (@$query->execute()) {
						$filled = true;
						$completed++;
					}
					else {
						$missed++;
					}

					// $stop = helper::microtime_float();

					// $time = $stop - $start;
					
					// helper::writeLine("Query processing time : ".$time." seconds",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
				}
				
				if ($resource["sql_after_import"]) {
					@$this->db->query($resource["sql_after_import"]);
				}
			}

			return $filled;
		}
		
		/**
		* Helper method to make sure the importer float is in the right format for mysql
		* @param 	Float String 	$float_string 	the string to be fixed and converted
		* @return 	Mysql Float 		$mySqlFloat 		the converted float number
		*/
		private function stringToMySqlFloat($float_string) {
			$mySqlFloat = str_replace(",",".",$float_string);
			$mySqlFloat = floatval($mySqlFloat);
			return $mySqlFloat;
		}

		/**
		* Helper method to make sure the importer date is in the right format for mysql datetime format
		* @param 	Date String 	$date_string 	the string to be fixed and converted
		* @return 	Mysql Date 		$mySqlDate 		the converted date
		*/
		private function stringToMySqlDate($date_string) 
		{
			//fix date format from 01/01/14 -> 01/01/2014 
			$date_string = preg_replace("/^([0-9]{2})\/([0-9]{2})\/([0-9]{2})$/","\$1/\$2/20\$3", $date_string);

			//replacing the "/" with the "-" strtotime() understands that we want the European Format
			$mySqlDate = date("Y-m-d H:i:s",strtotime(str_replace("/","-",$date_string)));
			return $mySqlDate;
		}

		/**
		* It converts a csv to an 2D array
		* @param 	Local Path 		the local path of the csv file
		* @param 	Has Header 		true if the csv has a header, false otherwise
		* @param 	Delimiter 		the delimiter of the csv
		* @param 	Enclosure 		the enclosure of the csv
		* @return 	2D Array 		the created 2D array from the csv
		*/
		private function csvToArray($local_path, $has_header, $delimiter, $enclosure) {
		
			$success = false;
			$data = array();

			if(file_exists($local_path) && is_readable($local_path)) {
				$handle = fopen($local_path, 'r');
				if ($handle) {
					$first_row = $has_header;
					while (($row = fgetcsv($handle, 1000, $delimiter, $enclosure)) !== FALSE) {
						if($first_row) {
							$first_row = false;
						}
						else {
							$data[] = $row;
						}
					}			
					fclose($handle);
				}
			}
			return $data;
		}

		/**
		* This moethods allows to know if a certain resource has to be imported or not
		* @param 	Resource 		$resource 		the resource to be imported
		* @return 	Current Time 	$current_time 	the time when the import process started
		*/
		private function hasToBeImported($resource,$current_time)
		{
			$sync_time = $resource['sync_time'] ? json_decode($resource['sync_time']) : array();
			$time_list = self::syncTimeToTimeList($sync_time);
			return (in_array($current_time,$time_list));
		}

		/**
		* Converts short format time to list of times 
		* @param 	Short Time Format 	$sync_times 			the short time format
		* @return 	Time List 			$complete_time_list 	array of times
		*/
		private function syncTimeToTimeList($sync_times)
		{
			$complete_time_list = array();

			foreach ($sync_times as $sync_time) {

				$time_list = array();

				$sync_time = str_replace(":","",$sync_time);
				
				if ($sync_time[0] == "*") {
					$time_list[] = "0";
					$time_list[] = "1";
					$time_list[] = "2";
				}
				else {
					$time_list[] = $sync_time[0];
				}

				if ($sync_time[1] == "*") {
					foreach ($time_list as $key => $time) {
						if ($time[0] == "2") {
							$time_list[] = "20";
							$time_list[] = "21";
							$time_list[] = "22";
							$time_list[] = "23";
						}
						else {
							for ($i=0; $i<10; $i++) {
								$time_list[] = $time[0].$i;
							}
						}
					}
				}
				else {
					foreach ($time_list as $key => $time) {
						$time_list[] = $time.$sync_time[1];
					}
				}

				if ($sync_time[2] == "*") {
					foreach ($time_list as $key => $time) {
						for ($i=0; $i<6; $i++) {
							$time_list[] = $time.$i;
						}
					}
				}
				else {
					foreach ($time_list as $key => $time) {
						$time_list[] = $time.$sync_time[2];
					}
				}

				if ($sync_time[3] == "*") {
					foreach ($time_list as $key => $time) {
						for ($i=0; $i<10; $i++) {
							$time_list[] = $time.$i;
						}
					}
				}
				else {
					foreach ($time_list as $key => $time) {
						$time_list[] = $time.$sync_time[3];
					}
				}

				foreach ($time_list as $key => &$time) {
					if (strlen($time) == 4) {
						$time = $time[0].$time[1].":".$time[2].$time[3];
						$complete_time_list[] = $time;
					}
					else {
						unset($time_list[$key]);
					}
				}
			}

			return $complete_time_list;
		}
		
		/**
		* Lock Resource function so that 2 proccesses can't import the same resource
		* @param 	Name 			$name 		The name of the resource to lock
		* @return 	Result 			$result 	The result of the lock 
		*/
		private function lockResource($name)
		{
			$query_str = "UPDATE system_resources SET locked = 1 WHERE local_name = '".$name."'";
			$query = $this->db->prepare($query_str);
			$result = $query->execute();

			return $result;

		}

		/**
		* Unlock Resource function so that a proccess gets unlocked after it got imported
		* @param 	Name 			$name 		The name of the resource to unlock
		* @return 	Result 			$result 	The result of the unlock 
		*/
		private function unlockResource($name)
		{
			$query_str = "UPDATE system_resources SET locked = 0 WHERE local_name = '".$name."'";
			$query = $this->db->prepare($query_str);
			$result = $query->execute();

			return $result;
		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".IMPORTER_ROOT." import auto\t\t\t\tImports all active resources that have to be imported now");
			helper::writeLine(E_SYNC_ROOT." ".IMPORTER_ROOT." import all\t\t\t\tImports all active resources");
			helper::writeLine(E_SYNC_ROOT." ".IMPORTER_ROOT." import [resource_name]\t\tImports [resource_name] if exists and is active");
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

			// self::syncTimeToTimeList(array("*1:00","*1:30"));
			// echo strtotime();

			die();
		}

	}

?>