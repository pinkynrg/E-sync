<?php 

	class helper {

		const 	ERROR_ALERT		= "error", 		// alerts types
				SYSTEM_ALERT	= "system", 	//
				SUCCESS_ALERT 	= "success", 	//
				TYPE_INTEGER 	= "integer", 	// input types
				TYPE_STRING  	= "string", 	//
				TYPE_HOST  		= "host",		//
				TYPE_PATH 		= "path", 		//
				TYPE_YES_NO  	= "yes/no",		//
				TYPE_TIME 		= "time",		//
				TYPE_ENTER 		= "enter",		//
				LOG_MSG 		= true, 		// to log messages or not
				NO_LOG_MSG 		= false, 		//
				LIVE_AFTER_MSG 	= false,		// behaviours after alerts
				DIE_AFTER_MSG	= true,			//
				TEMP 			= true,			// for saving downloads in temps folder instead of downlaods default folder
				SEPARATOR		= "-", 			// character used for line separator
				ARROW	 		= "→",			// arrow character
				BULLET_POINT 	= "●",			// for lists
				QUIT 			= ":Q"; 		// exit combination

		/**
		* Returns a PDO instance to the e_sync database
		* @param 	boolean 		$withExceptions 	True if the connection should throw exceptions
		* @return 	PDO Instance 	$pdo 	An instance to the e_sync database
		*/
		static function getPDOHandle($withExceptions = false) 
		{
			try {
				$pdo = new PDO('mysql:host='.DB_HOST.';dbname='.DB_NAME.';charset=utf8', DB_USER, DB_PASS);
				if ($withExceptions) {
					$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
					self::_registerErrorHandler();
				}
			} catch (Exception $e) {
				helper::writeLine("There are problems with the DB connection. The server says: ".$e->getMessage(),helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
			}
			return $pdo;
		}
		
		protected static $_isErrorHandlerRegistered = false;
		protected static function _registerErrorHandler()
		{
			if (!self::$_isErrorHandlerRegistered) {
				set_exception_handler(function ($exception) {
					helper::writeLine("$exception", helper::SUCCESS_ALERT, helper::LIVE_AFTER_MSG, helper::LOG_MSG);
				});
				$_isErrorHandlerRegistered = true;
			}
		}

		/**
		* method to see if a table exists or not
		* @param 	PDO Instance 	$pdo 			The database instance
		* @param 	Table Name 		$table_name 	The table name to look for
		* @return 	Existance 		$exists 		true if the table is found, false otherwise
		*/
		static function tableExists($pdo, $table_name) 
		{
			$query = $pdo->prepare("SHOW TABLES LIKE '$table_name'");
			$query->execute();
			$exists = (bool)$query->rowCount();
			return $exists;
		}

		/**
		* Returns the record in a certain table of a certain ID
		* @param 	Table 		$table 		The table where to check the ID
		* @param 	Id 			$id 		The id to be checked in the table
		* @return	Result 		$result	 	Corrisponding record if ID exists, false otherwise
		*/
		public function getRecordById($table, $id) 
		{
			$result = false;

			if (helper::tableExists($this->db, $table)) {
				$query = $this->db->prepare("SELECT * FROM $table WHERE id = '$id' LIMIT 1");
				$query->execute();
				$result = $query->fetch();
			}
			
			return $result;
		}

		/**
		* Get Resources by name
		* @param 	Resource Name 	$name 		The name of the resource to load, if null returns all active resources
		* @return 	Resources 		$resources  Array of all loaded resources
		*/
		public function getResources($name = null)
		{

			$query_str = "SELECT * FROM system_resources 
						  LEFT JOIN system_sources ON (system_resources.source_id = system_sources.id)
						  LEFT JOIN system_services ON (system_sources.service_id = system_services.id)
						  WHERE active = 1 AND locked = 0";

			if (!is_null($name)) $query_str .= " AND local_name = '".$name."'";

			$query = $this->db->prepare($query_str);
			$query->execute();
			$resources = $query->fetchAll();

			return $resources;
		}

		/**
		* Get the file extention from a file name
		* @param 	File Name 	$file_name 	The name of the file (example.txt)
		* @return 	Extension 	#extension 	The extension of the file (txt)
		*/
		static function getFileExtensionFromFileName($file_name)
		{
			$temp = explode(".",$file_name);
			$extention = $temp[count($temp)-1];
			return $extention;
		}

		/**
		* Get the file name from a path 
		* @param 	Path 		$path 		the path of the file (/Users/pippo/Desktop/example.txt)
		* @return 	File Name 	$file_name 	the name of the file (example.txt)
		*/
		static function getFileNameFromPath($path)
		{
			$exploded_path = explode(DS,$path);
			$file_name = $exploded_path[count($exploded_path)-1];
			return $file_name;
		}

		/**
		* Get the directory containing the file from a path
		* @param 	Path 	$path 	the path of the file (/Users/pippo/Desktop/example.txt)
		* @return 	Dir 	$dir 	the directory containing the file (/Users/pippo/Desktop)
		*/
		static function getDirFromFilePath($path)
		{
			$exploded_path = explode(DS,$path);
			$exploded_dir = array_slice($exploded_path,0,count($exploded_path)-1);
			$dir = implode(DS,$exploded_dir);
			return $dir;
		}

		/**
		* Executed to clear the screen
		* @param void
		* @return void
		*/
		static function clearScreen()
		{
			echo "\033[2J\033[1;1H";
		}

		/**
		* Returns a separator
		* @param 	void
		* @return 	Separator 	$separator 	the text separator
		*/
		static function getSeparator()
		{	
			$window_size = self::getTerminalSize();
			$separator = "";

			for ($i = 0; $i < $window_size['width']; $i++) {
				$separator .= self::SEPARATOR;
			}

			return $separator;
		}

		/**
		* Write a line to the user
		* @param 	Message 				$message 		The message to be printed
		* @param 	Type 					$type 			The message type to be printed
		* @param 	Die After Message 		$die 			If true aborts program otherwise continue
		* @param 	Next Line After Message $next_line 		If true next line after message, same line otherwise
		* @return 	void
		*/
		static function writeLine($msg, $type = null, $die = false, $log = false, $next_line = true) 
		{
			$window_size = helper::getTerminalSize();
			$time = "[".date("d-m-y H:i:s")."]";

			switch ($type) {
				case self::SYSTEM_ALERT		: $output = $time." SYSTEM: ".$msg; break;
				case self::SUCCESS_ALERT	: $output = $time." SUCCESS: ".$msg; break;
				case self::ERROR_ALERT		: $output = $time." ERROR: ".$msg; break;
				default 					: $output = $msg; break;
			}

			if ($log) {
				file_put_contents(LOGS, $time." ".$msg."\n", FILE_APPEND);
			}

			$output = "\r".$output;

			if ($next_line) $output .= "\n";

			$output_lines = explode("\n",$output);
			foreach ($output_lines as &$output_line) {
				$output_line = substr($output_line,0,$window_size['width']);
			}
			$output = implode("\n",$output_lines);

			printf($output);

			if ($die) die();
		}

		/**
		* Write a bulletpointed list
		* @param 	List 	$list 	1D array to be printed in a bulletpointed list
		* @return 	void
		*/
		static function writeList($list) 
		{
			self::writeLine("");
			foreach ($list as $point) {
				self::writeLine(self::BULLET_POINT." ".$point);
			}
			self::writeLine("");
		}

		/**
		* Ask Something to user and get an answer from standard input
		* @param 	Question 	$question 	The question to be asked to the used 
		* @param 	Type 		$type 		The type that the answer has to respect
		* @return 	Answer 		$answer 	The answer from the user
		*/
		static function askUser($question, $type = "string") 
		{
			$output = "[QUIT=".self::QUIT."] ".$question." ";
			
			switch ($type) {
				case self::TYPE_YES_NO 	: $output = $output."(yes/no) "; break;
				case self::TYPE_TIME 	: $output = $output."[HH:MM] * valid "; break;
			}

			self::writeLine($output,null,false,false,false);
			
			$handle = fopen('php://stdin', 'r');
			$answer = trim(fgets($handle));

			if ($answer == self::QUIT) die();

			$valid = self::validateAnswer($answer,$type);
			
			if (!$valid){
				self::writeLine("The given answer doesn't seem to be conform to the request.",helper::ERROR_ALERT,helper::LIVE_AFTER_MSG);
				$answer = self::askUser($question,$type);
			}
			
			return $answer;
		}

		/**
		* Method to let the user read carefully before continuing 
		* @param 	void
		* @return 	void
		*/
		static function typeEnterToContinue()
		{
			self::askUser("Type Enter to continue...",self::TYPE_ENTER);
		}

		/** 
		* Validate user answer in the terminal
		* @param  	Answer 		$answer 	The answer forn the user
		* @param 	Type 		$type 		The type it has to be respected
		* @return 	Valid 		$valid 		Validity of user input
		*/
		static function validateAnswer($answer, $type) 
		{
			switch ($type) {
				case self::TYPE_INTEGER 	: $valid = self::validateInteger($answer); break;
				case self::TYPE_STRING		: $valid = self::validateString($answer); break;
				case self::TYPE_HOST 		: $valid = self::validateHost($answer); break;
				case self::TYPE_PATH 		: $valid = self::validatePath($answer); break;
				case self::TYPE_YES_NO 		: $valid = self::validateYesNo($answer); break;
				case self::TYPE_TIME 		: $valid = self::validateTime($answer); break;
				case self::TYPE_ENTER 		: $valid = true; break;
				default 					: self::writeLine("Type not defined for validation"); $valid = false; break;
			}

			return $valid;
		}

		/**
		* Validate a host user input
		* @param 	Host 	$host 		The input from user
		* @return 	Result 	$result  	The result of the host validation		
		*/
		static function validateHost($host) 
		{
			preg_match("/^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/", $host, $matched_ip);
			preg_match("/^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$/", $host, $matched_host);
			$result = (isset($matched_ip[0]) && strlen($matched_ip[0]) ) || (isset($matched_host[0]) && strlen($matched_host[0]) );
			return $result;
		}

		/**
		* Validate a path user input
		* @param 	Path 	$path 	The input from user
		* @return 	Result 	$result  	The result of the host validation		
		*/
		static function validatePath($path) 
		{
			preg_match("/^(\/){1}([a-zA-Z0-9\.\-\_]+\/)*[a-zA-Z0-9\.\-\_]+$/",$path,$matched_path);
			$result = (isset($matched_path[0]) && strlen($matched_path[0]));
			return $result;
		}

		/**
		* Validate a string user input
		* @param 	String 	$string 	The input from user
		* @return 	Result 	$result  	The result of the string validation		
		*/
		static function validateString($string) 
		{
			$result = (bool)strlen($string);
			return $result;
		} 

		/**
		* Validate an integer user input
		* @param 	Integer 	$integer 	The input from user
		* @return 	Result 		$result  	The result of the integer validation		
		*/
		static function validateInteger($integer) 
		{
			$result = is_numeric($integer);
			return $result;
		}

		/**
		* Validate a yes/no user input
		* @param 	Answer 	$answer 	The input from user
		* @return 	Result 	$result  	The result of the yes/no validation		
		*/
		static function validateYesNo($answer) 
		{
			$result = (bool)($answer == "yes" || $answer == "no");
			return $result;
		}

		/**
		* Validate a time user input
		* @param 	Time 	$time 		The input from user
		* @return 	Result 	$result  	The result of the time validation		
		*/
		static function validateTime($time)
		{
			preg_match("/^(\*[0-9\*]|0[0-9\*]|1[0-9\*]|2[0-3\*]):[0-5\*][0-9\*]$/",$time,$matched_time);
			$result = isset($matched_time[0]);
			return $result;
		}

		/**
		* Get the terminal size
		* @param 	void
		* @return  	Size 	$size 	an array with eight and width of the screen
		*/
		static function getTerminalSize() 
		{
			$size = array('width' => 80, 'height' => 24);
			if (php_sapi_name() === 'cli' && strtoupper(substr(PHP_OS, 0, 3)) !== 'WIN') {
				$size['width'] = exec('tput cols');
				$size['height'] = exec('tput lines');
			}
			return $size;
		}

		/**
		* Get the time in seconds to measure exe time (end - start = exe time)
		* @param 	void
		* @return  	Time 	$time 	float time in seconds
		*/		
		public function microtime_float() {
	    	list($usec, $sec) = explode(" ", microtime());
	    	$time = ((float)$usec + (float)$sec);
	    	return $time;
		}
	}

?>