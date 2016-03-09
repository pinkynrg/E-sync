<?php 

	class sourceManager {

		const DEBUG = false; // if this variable is set to true just the debug block at the end of the class will be executed

		/**
		* SourceManager Class Constructor
		* @param 	void
		* @return 	void
		*/
		function __construct() 
		{
			$this->db = helper::getPDOHandle();
			$result = self::createCoreTables();
			if (self::DEBUG) self::debugBlock();
			if (!$result) die();
		}

		/**
		* Add host, username, password, service for communicating with new source
		* @param 	void
		* @return 	void
		*/
		public function actionAddSource() 
		{
			$result = false;

			do {
				if (isset($pinged)) unset($pinged);
				$host = helper::askUser("[LOCALHOST = 127.0.0.1] Type in the name of the host [esempio: dataservice.esprinet.com or and IP like 221.56.12.2]:",helper::TYPE_HOST);
				self::showList("system_services");	

				do {
					$id = helper::askUser("Type in the ID of the service to use from one in the list:",helper::TYPE_INTEGER);
					$service = helper::getRecordById($this->db,"system_services",$id);
				} while (!$service);

				if (strtolower($host) != "localhost" && strtolower($host) != "127.0.0.1") {
					$pinged = self::ping($host,$service['port']);
					if (!$pinged) {
						helper::writeLine("The service doesn't seem to be responding",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
						$repeat = helper::askUser("Do you want to try to reinsert the source data?",helper::TYPE_YES_NO);
						if ($repeat == "no") {
							helper::writeLine("The operation was aborted",helper::SYSTEM_ALERT,helper::DIE_AFTER_MSG);
						}
					}
					else {
						helper::writeLine("The service seems to be responding to the handshake",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
					}
				}

			} while (isset($pinged) && !$pinged);

			$user = helper::askUser("[SKIP = -1] Type in the username to access the resource:",helper::TYPE_STRING);
			$user = $user == -1 ? "" : $user;
			$pass = helper::askUser("[SKIP = -1] Type in the password to access the resoure:",helper::TYPE_STRING);
			$pass = $pass == -1 ? "" : $pass;
			$name = helper::askUser("Type in the name of the source [esempio: Esprinet]:",helper::TYPE_STRING);

			$query = $this->db->prepare("INSERT INTO system_sources (name,host,username,password,service_id) VALUES ('".$name."','".$host."','".$user."','".$pass."','".$service['id']."')");
			$result = $query->execute();

			if (!$result) {
				helper::writeLine("It hasn't been possible to add the new source",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
			}
			else {
				helper::writeLine("The source it has been added successfully!",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG);
			}
		}

		/**
		* Add new file to be imported from a certain source
		* @param 	void
		* @return 	void
		*/
		public function actionAddResource() 
		{
			$path = null;					// the path of the file
			$local_name = null;				// used to rename the file to the user desire
			$source_id = null;				// the file source id
			$conversion_rule = null; 		// the conversion the file has to go through
			$delimiter = null; 				// the delimiter of the csv
			$enclosure = null; 				// the enclosure of the csv
			$empty_lines = null;			// the number of empty lines in the csv
			$csv_has_header = null; 		// if the csv has a header row describing the content of the file
			$indices = array(); 			// list of indexes of the table
			$columns = array(); 			// list of columns and types of the table

			if (self::countRecords("system_sources")) {
				
				$source_id = self::getSourceId();
				$path = self::getResourcePath($source_id);
				$conversion = self::getConversionRule($source_id,$path);
				$conversion_rule = $conversion->rule;
				$local_path = $conversion->path;
				$local_name = self::getFileLocalName();
				$empty_lines = self::getNumberEmptyLine($local_path);
				$csv_has_header = self::getHasHeaderRow($local_path,$empty_lines);
				$delimiter = self::getCSVDelimiter($local_path,$empty_lines);
				$enclosure = self::getCSVEnclosure($local_path,$empty_lines,$delimiter);
				$columns['name'] = self::getColumnNamesCSV($csv_has_header, $local_path, $delimiter, $enclosure, $empty_lines);

				$columns['type'] = self::getColumnTypesCSV($local_path,$empty_lines,$columns['name'],$delimiter,$enclosure);
				$indices = self::getIndices($columns['name']);

				$columns = json_encode($columns);
				$indices = json_encode($indices);

				$query = $this->db->prepare("INSERT INTO system_resources (path,local_name,source_id,conversion_rule,delimiter,enclosure,csv_header_row,empty_lines,indices,columns) VALUES 
											('$path','$local_name','$source_id','$conversion_rule','$delimiter','$enclosure','$csv_has_header','$empty_lines','$indices','$columns')");

				$result = $query->execute();

				if (!$result) {
					helper::writeLine("INSERT INTO system_resources (path,local_name,source_id,conversion_rule,delimiter,enclosure,csv_header_row,empty_lines,indices,columns)\n 
										VALUES ('$path','$local_name','$source_id','$conversion_rule','$delimiter','$enclosure','$csv_has_header','$empty_lines','$indices',\n
										'$columns')",
										helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					helper::writeLine("It hasn't been possible to add the new resource",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
				}
				else {
					helper::writeLine("The resource it has been added successfully! ",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG);
					$answer = helper::askUser("Do you want to add another resource?",helper::TYPE_YES_NO);
					
					if ($answer == "yes") {
						self::actionAddResource();
					}
				}
			} 
			else {
				$answer = helper::askUser("There are no sources available to link the file to add.\nDo you want to insert a new source?",helper::TYPE_YES_NO);
				if ($answer == "yes") {
					if (self::actionAddSource()) {
						self::actionAddResource();
					}
				}
			}
		}

		/**
		* Remove a resource
		* @param 	void
		* @return 	void
		*/
		public function actionRemoveResource()
		{
			self::showList("system_resources");
			
			do {
				$resource_id = helper::askUser("What resource do you want to remove?",helper::TYPE_INTEGER);
				$resource = helper::getRecordById($this->db,"system_resources",$resource_id);
				if (!$resource) {
					helper::writeLine("The specified ID doens't seem to exist",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
				}
			} while (!$resource);

			$answer = helper::askUser("Are you sure you want to delete ".$resource['local_name']."?",helper::TYPE_YES_NO);

			if ($answer == "yes") {

				$query = $this->db->prepare("DELETE FROM system_resources WHERE id = '".$resource_id."'");
				$result = $query->execute();

				if ($result) {
					helper::writeLine("The Resource ".$resource['local_name']." has been removed successfully",helper::SUCCESS_ALERT,helper::DIE_AFTER_MSG);
				}
				else {
					helper::writeLine("The Resource ".$resource['local_name']." couldn't be removed",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
				}
			}
			else {
				helper::writeLine("The operation was aborted",helper::SYSTEM_ALERT,helper::DIE_AFTER_MSG);
			}
		}

		/**
		* Add a time of sync for a certain resource
		* @param 	void
		* @return 	void
		*/
		public function actionSetTimeofSync() 
		{
			$time_list = array();
			self::showList("system_resources");
			do {
				$resource_id = helper::askUser("Type the ID of the resource you want to set the sync of:",helper::TYPE_INTEGER);
				$valid = helper::getRecordById($this->db,"system_resources",$resource_id);
				if (!$valid) {
					helper::writeLine("The inserted ID doesn't seem to be correct",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
				}
			} while (!$valid);

			do {
				$time = helper::askUser("Write the time of the sync:",helper::TYPE_TIME);
				if ($time) {
					$repeat = helper::askUser("Do you want another insertion?",helper::TYPE_YES_NO);
					$time_list[] = $time;
				}
				else {
					helper::writeLine("Time NOT Correct!",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);			
				}
			} while (!$time || $repeat == "yes");

			$time_list = json_encode($time_list);

			$query = $this->db->prepare("UPDATE system_resources SET sync_time = '".$time_list."' WHERE Id = '".$resource_id."'");
			$result = $query->execute();

			if ($result) {
				helper::writeLine("The sync time was saved successfully",helper::SUCCESS_ALERT,helper::DIE_AFTER_MSG);
			}
			else {
				helper::writeLine("There was a problem saving the sync time",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);				
			}
		}

		/**
		* Check if a certain host responces to a certain services 
		* @param 	Host 		$host 		The host: it could be an IP or a hostname
		* @param 	Port 		$port 		The port on which runs the service
		* @return	Result 		$result	 	Result of the operation (true/false)
		*/
		private function ping($host,$port,$timeout=5)
		{
	        $result = @fsockopen($host, $port, $errno, $errstr, $timeout);
	        return $result;
		}
		
		/**
		* Get source ID from user when adding a new resource
		* @param 	void
		* @return 	Id 		$id 	the id of the source if exists, false otherwise
		*/
		private function getSourceId() 
		{
			self::showList("system_sources");
			
			do {
				$id = helper::askUser("Write the ID of the source where to add the file to download:",helper::TYPE_INTEGER);
				$source = helper::getRecordById($this->db,"system_sources",$id);
				if ($source == false) {
					helper::writeLine("The typed ID doesn't seem to be a valid source.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
				}
			} while (!$source);

			return $id;
		}

		/**
		* Get the resource path from the user
		* @param 	SourceId 	$source_id 	the id of the source where to add the resource
		* @return 	Path 		$path 		the path of the new resource
		*/
		private function getResourcePath($source_id)
		{
			$source = helper::getRecordById($this->db,"system_sources",$source_id);
			
			do {
				$path = helper::askUser("Write the relative path of the file (leading /):",helper::TYPE_PATH);
				$query = $this->db->prepare("SELECT * FROM system_resources WHERE path = '$path'");
				$query->execute();
				if ($query->rowCount()) {
					helper::writeLine("The specified resource is already loaded in the system",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
					$valid = false;
				}
				else {
					helper::writeLine("Trying to download the file. The time of this process depends on the size of the file and the used internet connection...",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
					$valid = self::downloadFile($source,$path);
					if ($valid) {
						helper::writeLine("The file seems to exists",helper::SUCCESS_ALERT,helper::LIVE_AFTER_MSG);
					}
					else {
						helper::writeLine("The file doens't seem to exists or the source access credetial might be wrong. ",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					}
				}
			} while (!$valid);
			
			return $path;
		}
		
		/**
		* Get the conversion rule for converting the imported file to a csv
		* @param 	SourceId 	$source_id 	the id of the source where to adc the resource
		* @param 	Path 		$path 		the path of the new resource
		* @return 	Conversion	$returned 	a Standard Object which has 2 attributes: the rule for converting the file and the path of the finally converted file
		*/
		private function getConversionRule($source_id,$path)
		{
			$returned = new stdClass();
			$conversions = array();
			$source = helper::getRecordById($this->db,"system_sources",$source_id);

			helper::writeLine("In order to be importable, the file needs to be in a CSV format.",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
			$to_convert = helper::askUser("Does the file need to be converted?",helper::TYPE_YES_NO);

			if ($to_convert == "yes") {

				helper::writeLine("Now we need to get a conversion path such as zip ".helper::ARROW." xls ".helper::ARROW." csv (meaning, unzip the resource and convert it from XLS to CSV).",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);

				helper::typeEnterToContinue();

				self::showList("system_valid_extensions");

				do {

					if (empty($conversions))
						$conversion_id = helper::askUser("Write the ID of the current format of the file:",helper::TYPE_STRING);
					else 
						$conversion_id = helper::askUser("Write the ID of the next format of the file:",helper::TYPE_STRING);

					if ($conversion = helper::getRecordById($this->db,"system_valid_extensions",$conversion_id)) {
						$duplicated = in_array($conversion['extension'],$conversions);
						if ($duplicated) {
							helper::writeLine("This conversion seems to be already been picked",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
						}
						else {
							$conversions[] = $conversion['extension'];
							$repeat = helper::askUser("Do you want to insert another conversion (currently ".implode(" ".helper::ARROW." ",$conversions)." ".helper::ARROW." csv) ?",helper::TYPE_YES_NO);
						}
					}
					else {
						helper::writeLine("The typed ID is not valid.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					}
				} while (!$conversion || $duplicated || $repeat == "yes");
			}
			
			if ($to_convert == "yes") {
				helper::writeLine("Now it will be shown part of the converted file",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
			}
			else {
				helper::writeLine("Now it will be shown part of the file",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
			}

			$conversions[] = "csv";
			$returned->rule = implode(helper::ARROW,$conversions);

			helper::typeEnterToContinue();

			$returned->path = self::convertFile($path,$returned->rule);

			if ($returned->path) {

				self::showFileChunk($returned->path);

				$is_csv = helper::askUser("Does the file looks like a CSV file?",helper::TYPE_YES_NO);

				if ($is_csv == "no") {
					
					if ($to_convert) {
						$repeat = helper::askUser("Do you want to try to find a conversion rule again?",helper::TYPE_YES_NO);
					}
					else {
						$repeat = helper::askUser("Do you want to find a conversion rule for this file?",helper::TYPE_YES_NO);
					}
					
					if ($repeat == "yes") {
						$returned = self::getConversionRule($source_id,$path);
					}
					else {
						helper::writeLine("The program will abort",helper::SYSTEM_ALERT,helper::DIE_AFTER_MSG);
					}
				}
			} 
			else {
				helper::writeLine("The convresion rule doesn't look to be right",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
				$repeat = helper::askUser("Do you want to try to find a conversion rule again?",helper::TYPE_YES_NO);
				if ($repeat) {
					$returned = self::getConversionRule($source_id,$path);
				}
				else {
					helper::writeLine("The program will abort",helper::SYSTEM_ALERT,helper::DIE_AFTER_MSG);
				}
			}

			return $returned;
		}

		/**
		* Get the local name and the DB table name for the new resource
		* @param 	void
		* @return 	Name	$name 	the local name and the DB table of the new resource
		*/
		private function getFileLocalName()
		{
			do {
				$name = helper::askUser("Write the file name that will be used locally:",helper::TYPE_STRING);
				$query = $this->db->prepare("SELECT * FROM system_resources WHERE local_name = '$name'");
				$query->execute();
				if ($query->rowCount()) {
					helper::writeLine("This name has already been taken by another resource.",helper::SYSTEM_ALERT);
					$name = false;
				}
			} while (!$name);
			
			return $name;
		}

		/**
		* Get the number of empty/irrelevant lines of the new csv resource 
		* @param 	Path 		$local_path		the path of the locally downloaded and/or converted file
		* @return 	EmptyLines 	$empty_lines	the number of empty/irrelevant lines in the resource
		*/
		private function getNumberEmptyLine($local_path)
		{
			$chunk_length = 10;
			helper::writeLine("Some CSV file has some empty or irrelevant lines to be removed before the CSV parsing proccess",helper::SYSTEM_ALERT);
			helper::writeLine("CSV header and CSV content a relevant lines, everything else is considered irrelevant",helper::SYSTEM_ALERT);
			helper::typeEnterToContinue();
			do {
				if (isset($fixed)) unset($fixed);
				self::showFileChunk($local_path,0,$chunk_length);
				$empty_lines = helper::askUser("How many empty lines does the CSV preview have?",helper::TYPE_INTEGER);
				if ($empty_lines != 0) {
					self::showFileChunk($local_path,$empty_lines,$chunk_length);
					$fixed = helper::askUser("Is the CSV file shown without empty lines now?");
				}
			} while (isset($fixed) && $fixed == "no");
			
			return $empty_lines;
		}

		/**
		* Finds out if the csv resource has a header file
		* @param 	Path 			$local_path		the path of the locally downloaded and/or converted file
		* @param 	EmptyLines 		$empty_lines	the number of empty/irrelevant lines in the resource
		* @return 	CsvHasHeader 	$csv_has_header true if the csv has a header row, false otherwise
		*/
		private function getHasHeaderRow($local_path, $empty_lines)
		{
			$chunk_length = 10;
			self::showFileChunk($local_path,$empty_lines,$chunk_length);
			$csv_has_header = helper::askUser("Does the file contains a header row?",helper::TYPE_YES_NO);
			$csv_has_header = $csv_has_header == "yes" ? "1" : "0";
			
			return $csv_has_header;
		}

		/**
		* Get the delimiter of the csv resource file
		* @param 	Path 			$local_path		the path of the locally downloaded and/or converted file
		* @param 	EmptyLines 		$empty_lines	the number of empty/irrelevant lines in the resource
		* @return 	Delimiter	 	$delimiter		the delimiter of the csv resource file
		*/
		private function getCSVDelimiter($local_path, $empty_lines)
		{
			$chunk_length = 10;
			do {
				self::showFileChunk($local_path,$empty_lines,$chunk_length);
				$delimiter = helper::askUser("Write the delimiter or the CSV (Examples: | ; , \\t):",helper::TYPE_STRING);
				$delimiter = str_replace("\\t","\t",$delimiter);
				$line = self::getFileChunk($local_path,$empty_lines,1);
				$values = str_getcsv($line,$delimiter);
				foreach ($values as &$value) {
					$value = trim($value);
				}
				helper::writeList($values);
				$correct = helper::askUser("Did the delimiter exploded the first line of the CSV correctly (You should see a list of N elements where N is the number of columns in the CSV)?",helper::TYPE_YES_NO);
			} while ($correct == "no");

			return $delimiter;
		}

		/**
		* Get the enclosure of the csv resource file
		* @param 	Path 			$local_path		the path of the locally downloaded and/or converted file
		* @param 	EmptyLines 		$empty_lines	the number of empty/irrelevant lines in the resource
		* @param 	Delimiter	 	$delimiter		the delimiter of the csv resource file
		* @return 	Enclosure 		$enclosure 		the enclosure of the csv resource file, empty string if none
		*/
		private function getCSVEnclosure($local_path, $empty_lines, $delimiter)
		{
			$chunk_length = 10;
			do {
				self::showFileChunk($local_path,$empty_lines,$chunk_length);
				$enclosure = helper::askUser("[NO ENCLOSURE = -1] Write the enclosure or the CSV (Examples: \" '):",helper::TYPE_STRING);
				if ($enclosure != -1) {
					$line = self::getFileChunk($local_path,$empty_lines,1);
					$values = str_getcsv($line,$delimiter,$enclosure);
					foreach ($values as &$value) {
						$value = trim($value);
					}
					helper::writeList($values);
					$correct = helper::askUser("Has the enclosure got removed correctly from the csv?",helper::TYPE_YES_NO);			
				}
			} while ($enclosure != "-1" && $correct == "no");

			$enclosure = $enclosure == "-1" ? "\"" : $enclosure;

			return $enclosure;
		}

		/**
		* Get the column names of the csv resource file
		* @param 	CsvHasHeader 	$csv_has_header true if the csv has a header row, false otherwise
		* @param 	Path 			$local_path		the path of the locally downloaded and/or converted file
		* @param 	Delimiter	 	$delimiter		the delimiter of the csv resource file
		* @param 	Enclosure 		$enclosure 		the enclosure of the csv resource file, empty string if none
		* @param 	EmptyLines 		$empty_lines	the number of empty/irrelevant lines in the resource
		* @return 	ColumnNames 	$column_names 	an array of elements with the names of the csv columns
		*/
		private function getColumnNamesCSV($csv_has_header, $local_path, $delimiter, $enclosure, $empty_lines) 
		{
			$header = self::getFileChunk($local_path,$empty_lines,1);

			$header_values = str_getcsv($header,$delimiter);
			$column_names = array();

			if ($csv_has_header) {
				$use_header_row = helper::askUser("Do you want to use the header in the CSV to name the future columns in the DB?",helper::TYPE_YES_NO);
				if ($use_header_row == "yes") {
					foreach ($header_values as $header_value) {
						$column_names[] = trim($header_value);
					}
				}
			}

			if (!$csv_has_header || $use_header_row == "no") {
				$number_of_columns = count($header_values);
				for ($i=0;$i<$number_of_columns;$i++) {
					do {
						$column_name = helper::askUser("Write the name of the #".($i+1)." column: ",helper::TYPE_STRING);
						$duplicated = in_array($column_name, $column_names);
						if ($duplicated) {
							helper::writeLine("This name has already been used for another column",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
						}
					} while ($duplicated);
					 $column_names[$i] = $column_name;
				}
			}

			return $column_names;
		}

		/**
		* Get the column MySql types of the csv resource file
		* @param 	Path 			$local_path		the path of the locally downloaded and/or converted file
		* @param 	EmptyLines 		$empty_lines	the number of empty/irrelevant lines in the resource
		* @param 	ColumnNames 	$column_names 	an array of elements with the names of the csv columns
		* @param 	Delimiter	 	$delimiter		the delimiter of the csv resource file
		* @param 	Enclosure 		$enclosure 		the enclosure of the csv resource file, empty string if none
		* @return 	ColumnTypes 	$column_types 	the MySql types for each column of the csv resource
		*/	
		private function getColumnTypesCSV($local_path,$empty_lines,$column_names,$delimiter,$enclosure)
		{
			$chunk_length = 10;
			$some_data = array();
			$column_types = array();
			$temp = explode("\n",self::getFileChunk($local_path,$empty_lines+1,$chunk_length));
			$columns_number = count($column_names);
			helper::writeLine("It is time to define the column types for each column once the file will getimported in the DB",helper::SYSTEM_ALERT);
			
			foreach ($temp as $record) {
				$values = str_getcsv($record,$delimiter,$enclosure);
				for ($i=0; $i<$columns_number;$i++) {
					$some_data[$i][] = trim($values[$i]);
				}
			}

			self::showList("system_valid_column_types");

			for($i=0; $i<$columns_number;$i++) {
				helper::writeLine(helper::getSeparator());
				helper::writeLine($column_names[$i]." ".helper::ARROW." ".implode(" - ",$some_data[$i]));
				helper::writeLine(helper::getSeparator());
				do {	
					$id_column_type = helper::askUser("Write the ID of the best matching type for the CSV sample data shown above:",helper::TYPE_INTEGER);
					$column_type = helper::getRecordById($this->db,"system_valid_column_types",$id_column_type);
					if ($column_type == false) {
						helper::writeLine("The typed ID doesn't seem to be a valid column type",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					}
					else {
						$column_types[] = $id_column_type;
					}
				} while (!$column_type);
			}
			
			return $column_types;
		}

		/**
		* Get the column MySql indices of the csv resource file
		* @param 	ColumnNames 	$column_names 	an array of elements with the names of the csv columns
		* @return 	Indices 	 	$indices 	 	the list of column names which will be making the role of indices in the MySql table
		*/	
		private function getIndices($column_names)
		{
			$indices = array();
			helper::writeLine("It is reccomended to add unique indices to avoid duplicate records and to create table which will be quickly querable",helper::SYSTEM_ALERT,helper::LIVE_AFTER_MSG);
			$add_indices = helper::askUser("Do you want to add any indices?");
			if ($add_indices == "yes") {
				helper::writeList($column_names);
				do {
					$column_name = helper::askUser("Type the name of the column you want to make index:",helper::TYPE_STRING);
					$valid = in_array($column_name, $column_names);
					if ($valid) {
						$duplicated = in_array($column_name,$indices);
						if ($duplicated) {
							helper::writeLine("The inserted column was already picked to be ad index",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
						}
						else {
							$indices[] = $column_name;
							$repeat = helper::askUser("Do you want to add another index?",helper::TYPE_YES_NO);
						}
					}
					else {
						helper::writeLine("The inserted column doesn't look to exist",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
					}
				} while (!$valid || $duplicated || $repeat == "yes");	
			}
			
			return $indices;
		}

		/**
		* Bridge function to downloader class to downlaod file from source
		* @param 	Source  	$source 	the mysql record source of the resource file
		* @param	File Path	$file 		remove path of the file
		* @return 	Success 	$success 	true if successfully got the file, false otherwise
		*/
		private function downloadFile($source, $path) 
		{	
			$resource = array();
			$service = helper::getRecordById($this->db,"system_services",$source['service_id']);

			$resource['service'] = $service['service'];
			$resource['port'] = $service['port'];
			$resource['host'] = $source['host'];
			$resource['username'] = $source['username']; 
			$resource['password'] = $source['password'];
			$resource['path'] = $path;

			$success = (bool)downloader::downloadFile($resource, helper::TEMP);

			return $success;
		}

		/**
		* Bridge function to downloader class to convert local file as requested from the user
		* @param 	Source  		$source 		the mysql record source of the resource file
		* @param 	RemotePath 		$path 			the remote path of the resource to download
		* @param 	Conversion 		$conversion 	the conversion rule to convert the resource file
		* @return	LocalPath		$local_path 	local path of the file if success, false otherwise
		*/
		private function convertFile($path, $conversion)
		{
			$resource = array();

			$resource['path'] = $path;
			$resource['conversion_rule'] = $conversion;

			$local_path = converter::convertFile($resource, helper::TEMP);

			return $local_path;
		}

		/**
		* Get a chunk of a text file
		* @param	LocalPath		$path 			local path of the file
		* @param 	Start 			$start 			the starting line of the file
		* @param 	NumberLines 	$number_lines	the length of the file chunk
		* @return 	Chunk 			$chunk 			the chunk of the file of false is something went wrong
		*/
		private function getFileChunk($path, $start = 0, $number_lines = 10) 
		{
			$lines = array();
			$chunk = false;

			$handle = fopen($path,"r");

			if ($handle) {

				for ($i=0; $i<$number_lines+$start && !feof($handle); $i++) {
					$line = fgets($handle);
					if ($i >= $start) {
						$clear_line = str_replace(array("\xEF\xBB\xBF","\n","\r"),"",$line);
						if ($clear_line != "") $lines[] = $clear_line;
					}
				}

				$chunk = implode("\n",$lines);
			}

			return $chunk;
		}

		/**
		* Creates the core tables necessary to run the program
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createCoreTables() 
		{
			$result = true;

			$result = self::createSystemServicesTable();

			if ($result) {
				$result = self::createSystemSourcesTable();
				if ($result) {
					$result = self::createSystemResourcesTable();
					if ($result) {
						$result = self::createSystemValidExtensionsTable();
						if ($result) {
							$result = self::createSystemValidColumnTypes();
							if (!$result) {
								helper::writeLine("The system_valid_column_types table coudn't be created",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
							}
						}
						else {
							helper::writeLine("The system_valid_extensions table coudn't be created",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
						}
					}
					else {
						helper::writeLine("The system_resources table coudn't be created",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
					}
				}
				else {
					helper::writeLine("The system_sources table coudn't be created",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
				}
			}
			else {
				helper::writeLine("The system_services table coudn't be created",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
			}

			return $result;
		}

		/**
		* Add table source to e_sync schema where all credetials are saved to access external resources
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createSystemSourcesTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db, "system_sources")) {
 				$query = $this->db->prepare("CREATE TABLE `system_sources` (
											  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
											  `name` varchar(100) NOT NULL DEFAULT '',
											  `host` varchar(100) NOT NULL DEFAULT '',
											  `username` varchar(100) NOT NULL DEFAULT '',
											  `password` varchar(100) NOT NULL DEFAULT '',
											  `service_id` int(11) unsigned NOT NULL,
											  PRIMARY KEY (`id`),
											  UNIQUE KEY `name` (`name`),
											  KEY `sources_services` (`service_id`),
											  CONSTRAINT `sources_services` FOREIGN KEY (`service_id`) REFERENCES `system_services` (`id`)
											) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;");

				$result = $query->execute();
			}

			return $result;
		}
		
		/**
		* Add table system_resources to e_sync schema where all where all files to be downloaded are stored
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createSystemResourcesTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db,"system_resources")) {
				$query = $this->db->prepare("CREATE TABLE `system_resources` (
											  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
											  `active` tinyint(1) NOT NULL DEFAULT '1',
											  `locked` tinyint(1) NOT NULL DEFAULT '0',
											  `path` varchar(100) NOT NULL DEFAULT '',
											  `local_name` varchar(100) NOT NULL,
											  `source_id` int(11) unsigned NOT NULL,
											  `conversion_rule` varchar(100) DEFAULT NULL,
											  `delimiter` varchar(5) DEFAULT NULL,
											  `enclosure` varchar(5) DEFAULT NULL,
											  `empty_lines` int(11) NOT NULL DEFAULT '0',
											  `csv_header_row` tinyint(1) NOT NULL,
											  `indices` text NOT NULL,
											  `columns` text NOT NULL,
										      `sync_time` varchar(100) DEFAULT NULL,
											  PRIMARY KEY (`id`),
											  UNIQUE KEY `path` (`path`),
											  UNIQUE KEY `local_name` (`local_name`),
											  KEY `resources_sources` (`source_id`),
											  CONSTRAINT `resources_sources` FOREIGN KEY (`source_id`) REFERENCES `system_sources` (`id`)
											) ENGINE=InnoDB DEFAULT CHARSET=utf8;");

				$result = $query->execute();

			}			

			return $result;
		}

		/**
		* Add table system_services to e_sync schema where all where all services are listed
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createSystemServicesTable() 
		{
			$result = true;

			if (!helper::tableExists($this->db, "system_services")) {
				$query = $this->db->prepare("CREATE TABLE `system_services` (
											  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
											  `service` varchar(10) NOT NULL DEFAULT '',
											  `name` varchar(50) NOT NULL DEFAULT '',
											  `port` int(11) DEFAULT NULL,
											  PRIMARY KEY (`id`)
											) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					$query = $this->db->prepare("INSERT INTO `system_services` (`id`, `service`, `name`, `port`) VALUES
												(1,'ftp','FTP (File Transfer Protocol)',21),
												(2,'http','HTTP (Hyper Text Transfer Protocol)',80),
												(5,'local','LOCAL (Get the file from local drive)',NULL);");

					$result = $query->execute();
				}
			}

			return $result;
		}

		/**
		* Add table system_valid_extensions to e_sync schema where all valid extension for conversion are stored
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createSystemValidExtensionsTable() 
		{
			$result = true;
			if (!helper::tableExists($this->db, "system_valid_extensions")) {
				$query = $this->db->prepare("CREATE TABLE `system_valid_extensions` (
											  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
											  `name` varchar(100) NOT NULL DEFAULT '',
											  `extension` varchar(10) NOT NULL DEFAULT '',
											  PRIMARY KEY (`id`),
											  UNIQUE KEY `name` (`name`)
											) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					$query = $this->db->prepare("INSERT INTO `system_valid_extensions` (`id`, `name`, `extension`) VALUES
												(1,'Zip: compressed file','zip'),
												(2,'Gzip: compressed file','gz'),
												(3,'Excel: xls or xlsx files are supported','xls');");
					$result = $query->execute();
				}
			}

			return $result;
		}
		
		/**
		* Add table system_valid_column_types to e_sync schema where all valid column types are stored
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function createSystemValidColumnTypes()
		{
			$result = true;
			if (!helper::tableExists($this->db, "system_valid_column_types")) {
				$query = $this->db->prepare("CREATE TABLE `system_valid_column_types` (
											  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
											  `name` varchar(20) NOT NULL DEFAULT '',
											  `dimension` varchar(20) NOT NULL DEFAULT '',
											  PRIMARY KEY (`id`),
											  UNIQUE KEY `name` (`name`)
											) ENGINE=InnoDB DEFAULT CHARSET=utf8;");

				$result = $query->execute();

				if ($result) {
					$query = $this->db->prepare("INSERT INTO `system_valid_column_types` (`id`, `name`, `dimension`) VALUES
												(1,'String','VARCHAR(250)'),
												(2,'Integer','INT'),
												(3,'Float','FLOAT(12,2)'),
												(4,'Date','DATE'),
												(5,'Description','TEXT');");

					$result = $query->execute();
				}
			}

			return $result;
		}

		/**
		* Count Elements in a table
		* @param 	Table 		$table 		The table where to count the records
		* @return	Result 		$result 	Result of the operation (true/false)
		*/
		private function countRecords($table) 
		{
			$query = $this->db->prepare("SELECT * FROM $table");
			$query->execute();
			return (bool)$query->rowCount();
		} 

		/**
		* Show a file chunk of a text file
		* @param	LocalPath		$path 			local path of the file
		* @param 	Start 			$start 			the starting line of the file
		* @param 	NumberLines 	$number_lines	the length of the file chunk
		* @return 	void
		*/
		private function showFileChunk($path, $start = 0, $number_lines = 10) 
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine(self::getFileChunk($path,$start,$number_lines));
			helper::writeLine(helper::getSeparator());
		}

		/**
		* Prints on the screen a list of a table
		* @param 	Table 		$table 		The table to be listed
		* @return	void
		* @throws 	Exception  				When it can't find the table 
		*/
		private function showList($table) 
		{
			if (helper::tableExists($this->db, $table)) {

				$query = $this->db->prepare("SELECT * FROM $table");
				$query->execute();
				$records = $query->fetchAll(PDO::FETCH_ASSOC);

				helper::writeLine(helper::getSeparator());

				foreach ($records as $record) {
					
					$line = array();

					foreach ($record as $key => $value) {
						if ($key == "id" || $key == "name" || $key == "local_name" || $key == "path") {
							$line[] = strtoupper($key).": ".$value;
						}
					}

					$line = implode("\t\t",$line);
					helper::writeLine($line);
				}

				helper::writeLine(helper::getSeparator());
			}
			else { 
				throw new Exception("Can't show list of $table because $table table doesn't Exists!");
			}
		}

		/**
		* Outputs help commands to the user
		*/
		public function help()
		{

			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".SOURCE_MANAGER_ROOT." addSource\t\tAdd an Addition Source (es.Esprinet) where to add resources later on");
			helper::writeLine(E_SYNC_ROOT." ".SOURCE_MANAGER_ROOT." addResource\t\tAdd an Addition Resource (es. categories, products lists ... ) linked to a certain Source");
			helper::writeLine(E_SYNC_ROOT." ".SOURCE_MANAGER_ROOT." removeResource\t\tRemove Resource");
			helper::writeLine(E_SYNC_ROOT." ".SOURCE_MANAGER_ROOT." setTimeofSync\t\tSet the sync time for a resource");
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

			// $columns = self::getColumnNamesCSV(1,DOWNLOADS.DS."listini.txt", ";","\"",2);
			// self::getColumnTypesCSV(DOWNLOADS.DS."listini.txt", 2, $columns, ";","\"");

			die();
		}
	}
?>