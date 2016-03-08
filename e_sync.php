<?php 

	require_once("classes/helper.php");
	require_once("classes/sourceManager.php");
	require_once("classes/downloader.php");
	require_once("classes/converter.php");
	require_once("classes/importer.php");
	require_once("classes/icecater.php");
	require_once("classes/flatter.php");
	require_once("config.php");

	class e_sync {

		private $arg1, $arg2, $arg3;

		public function __construct() {
			$this->db = helper::getPDOHandle();
		}

		/**
		* Route considering the 
		* @param 	void
		* @return 	void
		*/
		static public function bootstrap() 
		{
			global $argv;

			$controller = isset($argv[1]) ? $argv[1] : null;	
			$action 	= isset($argv[2]) ? "action".ucfirst($argv[2]) : null; 	
			$args 		= isset($argv[3]) ? array_slice($argv, 3) : null;

			$success = self::checkLogFile();
			if (!$success) {
				helper::writeLine("Something went wrong creating the log file",helper::ERROR_ALERT,helper::DIE_AFTER_MSG);
			}

			switch ($controller) 
			{
				case "--sourceManager"  : $controller = "sourceManager"; break;
				case "--downloader" 	: $controller = "downloader"; break;
				case "--converter" 		: $controller = "converter"; break;
				case "--importer" 		: $controller = "importer"; break;
				case "--icecater"		: $controller = "icecater"; break;
				case "--flatter" 		: $controller = "flatter"; break;
				case "--help" 			: self::help(); die();
				case "-h" 	 			: self::help(); die();
				default  				: self::help(); die();
			}

			if (method_exists($controller, $action)) {
				$class = new $controller($args);
				$class->$action();
			}
			else 
			{
				if (class_exists($controller)) {
					$class = new $controller();
					$class->help();
				}
				else {
					self::help();
				}
			}
		}

		/**
		* Helps user how to use this tool
		* @param 	void
		* @return 	void 		
		*/
		static private function help() 
		{
			helper::writeLine(helper::getSeparator());
			helper::writeLine("TYPE ONE OF THE FOLLOWING COMMANDS:\n");
			helper::writeLine(E_SYNC_ROOT." ".SOURCE_MANAGER_ROOT."\t\tSource And Resource Manager");
			helper::writeLine(E_SYNC_ROOT." ".DOWNLOADER_ROOT."\t\tDownloads all saved Resources");
			helper::writeLine(E_SYNC_ROOT." ".CONVERTER_ROOT."\t\tConverts downloaded resourced inside ".DOWNLOADS);			
			helper::writeLine(E_SYNC_ROOT." ".IMPORTER_ROOT."\t\tDownloads, Converts, and Imports Resources");
			helper::writeLine(E_SYNC_ROOT." ".ICECATER_ROOT."\t\tDownloads icecat links for flat tables");
			helper::writeLine(E_SYNC_ROOT." ".FLATTER_ROOT."\t\tCreates flat tables for out e-commerces");
			helper::writeLine(helper::getSeparator());
		}

		static private function checkLogFile()
		{
			$success = true;

			if (!file_exists(LOGS)) {

				$success = false;
				$dir = helper::getDirFromFilePath(LOGS);

				if (!is_dir($dir)) {
					$success = mkdir($dir);
				}
				
				if ($success) {
					$success = file_put_contents(LOGS, "");
				}
			}

			return $success !== false;
		}
	}

	e_sync::bootstrap();

?>